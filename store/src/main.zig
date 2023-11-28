const std = @import("std");
const File = std.fs.File;
const Allocator = std.mem.Allocator;

const Gpa = std.heap.GeneralPurposeAllocator(.{});

const ValueMetadata = struct {
    // TODO(Apaar): This makes this struct very heavy on memory! Could replace with a file ID
    // and while we're at it we could also replace the offset and len since the max segment size
    // is limited.
    file: *SegmentFile,
    offset: u64,
    len: u64,
};

const KeyToValueMetadataMap = std.StringHashMap(ValueMetadata);

// Use this to either read and process the value in-place or copy it elsewhere.
// Up to you.
//
// Actually I'm not sure this works because technically the file underneath could
// get swapped out if the background worker/thread compacts/merges segments.
//
// I should acquire a reader lock for that particular segment file and that should
// prevent it from being compacted for that duration. But that sort of defeats
// the scalability of the whole thing because now we can't compact while people
// are reading.
//
// I'd probably want some kinda refcounting for the files where we keep it around while
// there are still ValueProxy's in use for that file. This way, we can update our
// key to segment offset map immediately (sort of, we greatly limit the lock time) when we
// compact/merge.
//
// Let's say that our store gets bombarded with reads; this would still not be a problem because
// we'll swap out the segment map as soon as the compaction is completed and our `get` fn below
// returns. So we'll have only as many references to the original segment file as there were ValueProxies
// that called `get` in parallel; a number which is limited in practice by the number of cores on the machine.
const ValueProxy = struct {
    segment_file: *SegmentFile,
    offset: u64,
    len: u64,

    const Self = @This();

    pub fn deinit(self: *ValueProxy) void {
        // See the comment above `SegmentFile::unref` to see why it's kinda okay that we're doing this
        // for every read (not fantastic but better than an exclusive lock).
        self.segment_file.unref();
    }

    pub fn readInto(self: *const Self, out_buf: []u8) ![]u8 {
        std.debug.assert(out_buf.len == self.len);

        const count = try self.segment_file.file.preadAll(out_buf, self.offset);

        std.debug.assert(count == out_buf.len);

        return out_buf;
    }

    pub fn readAlloc(self: *const Self, allocator: Allocator) ![]u8 {
        const buf = try allocator.alloc(u8, self.len);
        return self.readInto(buf);
    }
};

const SegmentOpType = enum(u2) { set, del };

const SegmentOpHeader = union(SegmentOpType) {
    set: struct {
        key_len: u64,
        value_len: u64,
    },
    del: struct {
        key_len: u64,
    },
};

pub fn readSegmentOpHeader(file: *File) !?SegmentOpHeader {
    // TODO(Apaar): Initialize the map with the key and value offsets
    var op_type_key_len_buf: [1 + 8]u8 = undefined;

    const count = try file.readAll(&op_type_key_len_buf);

    if (count == 0) {
        return null;
    }

    const op_type: SegmentOpType = @enumFromInt(op_type_key_len_buf[0]);

    var key_len_buf: [8]u8 align(@alignOf(u64)) = undefined;
    @memcpy(&key_len_buf, op_type_key_len_buf[1..]);

    const key_len: u64 = @bitCast(key_len_buf);

    switch (op_type) {
        .set => {
            var value_len_buf: [8]u8 = undefined;
            _ = try file.readAll(&value_len_buf);

            const value_len: u64 = @bitCast(value_len_buf);

            return .{
                .set = .{
                    .key_len = key_len,
                    .value_len = value_len,
                },
            };
        },

        .del => {
            return .{
                .del = .{
                    .key_len = key_len,
                },
            };
        },
    }
}

const rand_path_bytes = 12;
const rand_path_len = std.fs.base64_encoder.calcSize(rand_path_bytes);
const RandPath = [rand_path_len]u8;

fn randPath() RandPath {
    var bytes: [rand_path_bytes]u8 = undefined;
    std.crypto.random.bytes(&bytes);

    var path: RandPath = undefined;
    _ = std.fs.base64_encoder.encode(&path, &bytes);

    return path;
}

fn timeInMillis() !u64 {
    var tv: std.os.timeval = undefined;
    try std.os.gettimeofday(&tv, null);

    return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

fn compareFilesLastModifiedTime(_: void, a: File, b: File) bool {
    const a_stat = a.stat() catch return false;
    const b_stat = b.stat() catch return true;

    return a_stat.mtime < b_stat.mtime;
}

// A File + non-atomic reference count.
// The reference count should only be modified within critical sections.
//
// Consider a scenario like the following:
//
// 1. Somebody calls `setAllocKey` on the store, which references the last segment file (i.e. the active one).
// 2. Context switch happens afterwards (when the segment_files/metadata lock is released) and then somebody else
// appends to the file (A), resulting in it hitting max_segment_size, at which point we insert another file (B) and now
// B is the last element in the segments array.
// 3. Another context switch, and now we're compacting. The compaction finishes and technically none of the files except
// the last one (B) and our newly compacted file contain required data; except, of course, the file that was referenced
// in step 1.
//
// Maintaining a reference count allows us to handle an arbitrarily long chain of threads which are holding onto
// a segment file that is no longer active.
//
// The secondary issue that arises is that we have our `ValueProxy` struct which holds onto these files as well. This
// means that we'll need to get a mutable lock before we can increment the refcount. Unless the refcount is atomic :)
//
// So yes, we'll use an atomic refcount on the file.
const SegmentFile = struct {
    allocator: Allocator,

    count: std.atomic.Atomic(usize) = std.atomic.Atomic(usize).init(1),

    dir: *std.fs.Dir,
    sub_path: []u8,

    delete_on_close: bool = false,

    file: File,

    // We should have either shared or exclusive ownership over
    // the file
    lock: std.Thread.RwLock,

    // SegmentFiles are heap-allocated because we want them to close/free themselves
    // once they are no longer referenced.
    fn init(allocator: Allocator, dir: *std.fs.Dir, sub_path: []const u8, file: File) !*SegmentFile {
        var self = try allocator.create(SegmentFile);

        self.* = SegmentFile{
            .allocator = allocator,
            .sub_path = try allocator.alloc(u8, sub_path.len),
            .lock = .{},
            .dir = dir,
            .file = file,
        };

        @memcpy(self.sub_path, sub_path);

        return self;
    }

    fn open(allocator: Allocator, dir: *std.fs.Dir, sub_path: []const u8, open_flags: File.OpenFlags) !*SegmentFile {
        return SegmentFile.init(allocator, dir, sub_path, try dir.openFile(sub_path, open_flags));
    }

    fn create(allocator: Allocator, dir: *std.fs.Dir, sub_path: []const u8, create_flags: File.CreateFlags) !*SegmentFile {
        return SegmentFile.init(allocator, dir, sub_path, try dir.createFile(sub_path, create_flags));
    }

    fn refAssumeLocked(self: *SegmentFile) void {
        _ = self.count.fetchAdd(1, .Monotonic);
    }

    // Assumes you do not have a lock on the file. Then, in the common case where
    // the refcount is still greater than zero, it just holds a shared lock.
    //
    // Once the refcount does hit zero, though, it grabs a writer lock; the nice thing
    // is that it's already uncontended in that case anyways (there aren't any readers
    // on another thread since the refcount hit zero), so it shouldn't slow anybody
    // down.
    fn unref(self: *SegmentFile) void {
        self.lock.lockShared();

        // The value _was_ 1 (fetchSub returns the old value) hence the if (true)
        if (self.count.fetchSub(1, .Release) == 1) {
            // Ensure that the decrement is visible to other cores prior to running the code hereafter
            self.count.fence(.Acquire);

            self.lock.unlockShared();

            // TODO(Apaar): Do we even need the exclusive lock here? No other thread should have a reference
            // to this file at this point?

            self.lock.lock();

            self.file.close();

            if (self.delete_on_close) {
                self.dir.deleteFile(self.sub_path) catch {};
            }

            self.allocator.free(self.sub_path);

            self.lock.unlock();

            // We call free outside of the lock since there's no race for the allocator.
            self.allocator.destroy(self);

            return;
        }

        self.lock.unlockShared();
    }
};

const Store = struct {
    allocator: Allocator,
    dir_path: []const u8,
    max_segment_size: u64,

    dir: *std.fs.IterableDir,

    // TODO(Apaar): Make a "Locked" struct or something
    lock: std.Thread.RwLock,

    segment_files: std.ArrayList(*SegmentFile),
    key_to_value_metadata: KeyToValueMetadataMap,

    const Self = @This();

    /// The store takes ownership of the given `dir`.
    pub fn init(allocator: Allocator, dir_path: []const u8, max_segment_size: u64) !Self {
        var dir_path_copy = try allocator.alloc(u8, dir_path.len);
        errdefer allocator.free(dir_path_copy);

        @memcpy(dir_path_copy, dir_path);

        // We have to use indirection here because we need the pointer to be stable (it escapes this scope and
        // store could move around anywhere).
        var dir_ptr = try allocator.create(std.fs.IterableDir);
        dir_ptr.* = try std.fs.cwd().makeOpenPathIterable(dir_path, .{});

        var self = Self{
            .allocator = allocator,
            .dir_path = dir_path_copy,
            .dir = dir_ptr,
            .lock = .{},
            .segment_files = std.ArrayList(*SegmentFile).init(allocator),
            .key_to_value_metadata = KeyToValueMetadataMap.init(allocator),
            .max_segment_size = max_segment_size,
        };

        errdefer self.deinit();

        var dir_iter = self.dir.iterate();

        // Load up every file in the directory
        while (try dir_iter.next()) |entry| {
            std.debug.assert(entry.kind == .file);

            try self.segment_files.append(try SegmentFile.open(
                allocator,
                &self.dir.dir,
                entry.name,
                // TODO(Apaar): Open every file as read only and then re-open as read-write below
                .{ .mode = .read_write },
            ));
        }

        // Find the smallest segment file that's below the threshold and then work on that; if none is found,
        // create one.
        //
        // The active segment file will always be the last one in the list.

        var min_size = max_segment_size;
        var smallest_segment_file_index: i32 = -1;

        for (self.segment_files.items, 0..) |segment_file, i| {
            const size = try segment_file.file.getEndPos();

            if (size < min_size) {
                smallest_segment_file_index = @intCast(i);
            }
        }

        if (smallest_segment_file_index < 0) {
            const path = randPath();

            var file = try SegmentFile.create(allocator, &self.dir.dir, &path, .{ .read = true });

            errdefer {
                file.delete_on_close = true;
                file.unref();
            }

            try self.segment_files.append(file);
        } else if (smallest_segment_file_index != self.segment_files.items.len - 1) {
            // Swap the smallest file to the end of the array
            std.mem.swap(
                *SegmentFile,
                &self.segment_files.items[@intCast(smallest_segment_file_index)],
                &self.segment_files.items[self.segment_files.items.len - 1],
            );
        }

        var temp_key_buf = std.ArrayList(u8).init(allocator);
        defer temp_key_buf.deinit();

        // Go through every segment op in every segment file and fill up our
        // key value metadata.
        for (self.segment_files.items) |segment_file| {
            while (try readSegmentOpHeader(&segment_file.file)) |header| {
                switch (header) {
                    .set => |op| {
                        // Read in the key
                        try temp_key_buf.resize(op.key_len);

                        _ = try segment_file.file.readAll(temp_key_buf.items);

                        const value_metadata = ValueMetadata{
                            // We should be at the value now since we read in the key above
                            .file = segment_file,
                            .offset = try segment_file.file.getPos(),
                            .len = op.value_len,
                        };

                        // Skip over the value
                        _ = try segment_file.file.seekBy(@intCast(op.value_len));

                        const entry = try self.key_to_value_metadata.getOrPutAdapted(
                            @as([]const u8, temp_key_buf.items),
                            self.key_to_value_metadata.ctx,
                        );

                        if (!entry.found_existing) {
                            // Copy it into our own non-temp buffer under our allocator
                            const key_buf = try allocator.alloc(u8, op.key_len);
                            @memcpy(key_buf, temp_key_buf.items);

                            entry.key_ptr.* = key_buf;
                        }

                        entry.value_ptr.* = value_metadata;
                    },

                    .del => |op| {
                        // Read in the key
                        try temp_key_buf.resize(op.key_len);

                        _ = try segment_file.file.readAll(temp_key_buf.items);

                        const removed = self.key_to_value_metadata.fetchRemove(temp_key_buf.items);

                        if (removed) |entry| {
                            // TODO(Apaar): Do not assume we always own the keys
                            self.allocator.free(entry.key);
                        }
                    },
                }
            }
        }

        return self;
    }

    pub fn deinit(self: *Self) void {
        // TODO(Apaar): What happens if multiple threads call deinit?
        // I guess there's no race but calling deinit twice is just bad, so
        // I'll leave this as-is.

        self.lock.lock();
        defer self.lock.unlock();

        var entry_iter = self.key_to_value_metadata.iterator();

        while (entry_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }

        self.key_to_value_metadata.deinit();

        for (self.segment_files.items) |segment_file| {
            segment_file.unref();
        }

        self.segment_files.deinit();

        self.dir.close();
        self.allocator.destroy(self.dir);

        self.allocator.free(self.dir_path);
    }

    // KATYA:
    // Maybe ValueProxy goes stale (underlying file is deleted/compacted), then
    // you can just retry (we indicate that it's stale).
    //
    // Technically we don't even have to guarantee that a ValueProxy for a given `get`
    // produces the value _at the time that the get happened_. We can even produce values
    // from the future (if that's fine).
    //
    // We may want to store a timestamp of when the key was written so we can e.g. provide
    // guarantees that no key/values more recent than e.g. x minutes will ever go stale.
    pub fn get(self: *Self, key: []const u8) ?ValueProxy {
        // TODO(Apaar): We could eventually shard the keyspace to better scale concurrent reads and writes

        // FIXME(Apaar): Deadlock potential here
        self.lock.lockShared();
        defer self.lock.unlockShared();

        const value_metadata = self.key_to_value_metadata.get(key) orelse return null;

        var file = value_metadata.file;

        file.lock.lockShared();
        defer file.lock.unlockShared();

        // This ref is for the ValueProxy
        file.refAssumeLocked();

        return .{
            .segment_file = file,
            .offset = value_metadata.offset,
            .len = value_metadata.len,
        };
    }

    pub fn setAllocKey(self: *Self, key: []const u8, value: []const u8) !void {
        const op_type_buf = [_]u8{@intFromEnum(SegmentOpType.set)};
        const key_len_buf: [8]u8 = @bitCast(key.len);
        const value_len_buf: [8]u8 = @bitCast(value.len);

        var iovecs = [_]std.os.iovec_const{
            .{
                .iov_base = &op_type_buf,
                .iov_len = op_type_buf.len,
            },
            .{
                .iov_base = &key_len_buf,
                .iov_len = key_len_buf.len,
            },
            .{
                .iov_base = &value_len_buf,
                .iov_len = value_len_buf.len,
            },
            .{
                .iov_base = key.ptr,
                .iov_len = key.len,
            },
            .{
                .iov_base = value.ptr,
                .iov_len = value.len,
            },
        };

        var file = blk: {
            // We need an exclusive lock because we don't want the file shifting around
            // underneath us while we grab the active one.
            self.lock.lock();
            defer self.lock.unlock();

            var file = self.segment_files.getLast();

            file.lock.lockShared();
            defer file.lock.unlockShared();

            // Make sure to increment the refcount before unlocking our exclusive lock because
            // it could no longer be referenced by segment_files.items after we unlock.
            //
            // Note that this ref is _not_ for the pointer we put into ValueMetadata! That could
            // lead to having lots of refs to a file (one for every single key in the file). Instead,
            // I treat the entire store + the scope of this function/thread as one ref each.
            file.refAssumeLocked();

            break :blk file;
        };

        // We ref'd it above for the sake of this thread that's writing, so unref once we're outta here.
        defer file.unref();

        const pos = blk: {
            // We take an exclusive lock on the file to prevent writes from interleaving
            file.lock.lock();
            defer file.lock.unlock();

            const pos = try file.file.getPos();
            try file.file.writevAll(&iovecs);

            break :blk pos;
        };

        const value_metadata = ValueMetadata{
            .file = file,
            .offset = pos + op_type_buf.len + key_len_buf.len + value_len_buf.len + key.len,
            .len = value.len,
        };

        {
            self.lock.lock();
            defer self.lock.unlock();

            const entry = try self.key_to_value_metadata.getOrPutAdapted(key, self.key_to_value_metadata.ctx);

            if (!entry.found_existing) {
                var key_copy = try self.allocator.alloc(u8, key.len);
                @memcpy(key_copy, key);

                entry.key_ptr.* = key_copy;
            }

            entry.value_ptr.* = value_metadata;
        }

        if (value_metadata.offset + value.len > self.max_segment_size) {
            // Append a new segment file because this last one is too big
            const path = randPath();

            // HACK(Apaar): Looks like WSL doesn't have the timestamp resolution necessary to sort the files correctly.
            // FIXME(Apaar): We should probably just come up with a path that is lexicographically greater than the
            // path of the active segment file. Then we can sort them lexicographically in the compaction step and call it
            // a day.
            //
            // Right now I'll just sleep for 10 milliseconds lol.
            std.time.sleep(10_000_000);

            var new_file = try SegmentFile.create(self.allocator, &self.dir.dir, &path, .{ .read = true });
            errdefer new_file.unref();

            self.lock.lock();
            defer self.lock.unlock();

            try self.segment_files.append(new_file);
        }
    }

    pub fn del(self: *Self, key: []const u8) !bool {
        var file = blk: {
            self.lock.lock();
            defer self.lock.unlock();

            // We do not record a delete operation if the key does not exist because that means a delete
            // operation was already recorded prior.
            const removed = self.key_to_value_metadata.fetchRemove(key) orelse return false;

            // TODO(Apaar): Do not assume we always own the keys
            self.allocator.free(removed.key);

            var file = self.segment_files.getLast();

            file.lock.lockShared();
            defer file.lock.unlockShared();

            file.refAssumeLocked();

            break :blk file;
        };

        const op_type_buf = [_]u8{@intFromEnum(SegmentOpType.del)};
        const key_len_buf: [8]u8 = @bitCast(key.len);

        var iovecs = [_]std.os.iovec_const{
            .{
                .iov_base = &op_type_buf,
                .iov_len = op_type_buf.len,
            },
            .{
                .iov_base = &key_len_buf,
                .iov_len = key_len_buf.len,
            },
            .{
                .iov_base = key.ptr,
                .iov_len = key.len,
            },
        };

        defer file.unref();

        // We need an exclusive lock while writing to prevent writes from interleaving.
        // Note that we do this after the `defer file.unref` to ensure th unref happens
        // _after_ we've unlocked the file; otherwise we'd deadlock.
        file.lock.lock();
        defer file.lock.unlock();

        try file.file.writevAll(&iovecs);

        return true;
    }

    // TODO(Apaar): Implement compaction. Just pick a random segment that's not the last segment
    // and compact it? Or maybe we should just have each compaction run as eagerly as possible? Try
    // to compact and merge as many segments as possible:
    //
    // - Create a hashtable just for processing our compaction.
    // - Loop through every non-active segment file (i.e. everything except the last segment) and
    // build up this hash table same as our init function for this store. We can avoid allocating and
    // copying new keys if we're willing to lock our `self.key_to_value_metadata` map and look up new keys.
    //
    // The whole point of this is to avoid locking at all during compaction otherwise
    // I'd just loop through our existing hashtable.
    //
    // Let's say we just copy keys. It's a little memory intensive but we might be able to do a thing where
    // if we're about to run out of memory we just dump our hashtable so far.
    //
    // Anyways we build up this hash table and then we can iterate through it, ...
    //
    // Well, regardless of what we do, I don't want to lock our main hash table ever. How can we avoid this?
    // I guess I could use a pointer to refer to the hash table, and then swap out that pointer atomically
    // once we've built up a new hashtable that has compacted data. We'd still need to do an atomic read of that
    // table but that's probably better than locking.
    //
    // I wanna see what the perf of an uncontended lock vs an atomic load is. Maybe it's not worth the complexity
    // because the only time we'd lock is very briefly at the end of the compaction.
    //
    // TO BE CONTINUED

    pub fn compactForMillis(self: *Self, ms: u64) !void {
        // FIXME(Apaar): There's a point of no return with the errors here. If we run OOM after compaction and deleting
        // the old files, then we'll need to restart. Need to be explicit about this and assert in some cases.
        //
        // Go through the error conditions below and do so.
        _ = ms;

        // TODO(Apaar): Implement cancellation after a certain amount of time has elapsed

        // Append all the compacted stuff into a single file as much as possible, no need to care about
        // max_segment_size for an "inactive" segment.
        const compacted_file_path = randPath();

        var compacted_file = try SegmentFile.create(self.allocator, &self.dir.dir, &compacted_file_path, .{ .read = true });
        errdefer compacted_file.unref();

        var temp_key_buf = std.ArrayList(u8).init(self.allocator);
        defer temp_key_buf.deinit();

        // We're gonna store the ranges of every set operation we care about and what file
        // they originated from so we can compact as many of them as possible into a single file.
        var compactable_files = std.ArrayList(File).init(self.allocator);
        defer {
            for (compactable_files.items) |file| {
                file.close();
            }

            compactable_files.deinit();
        }

        const SetOpRange = struct {
            compactable_file_index: u16,
            pos: u64,
            size: u64,

            // Used for reconstructing a new key_to_value_metadata map.
            value_offset_relative_to_pos: u64,
        };

        var key_to_set_op_range = std.StringHashMap(SetOpRange).init(self.allocator);
        defer {
            var key_iter = key_to_set_op_range.keyIterator();
            while (key_iter.next()) |key| {
                self.allocator.free(key.*);
            }

            key_to_set_op_range.deinit();
        }

        var dir_iter = self.dir.iterate();

        while (try dir_iter.next()) |dir_entry| {
            // TODO(Apaar): Do not assume the nested files are always regular files
            std.debug.assert(dir_entry.kind == .file);

            // Skip our compacted_file
            if (std.mem.eql(u8, dir_entry.name, &compacted_file_path)) {
                continue;
            }

            var file = try self.dir.dir.openFile(dir_entry.name, File.OpenFlags{ .mode = .read_only });

            if (try file.getEndPos() < self.max_segment_size) {
                // This is probably the active file, skip it
                file.close();
                continue;
            }

            try compactable_files.append(file);
        }

        if (compactable_files.items.len == 0) {
            return;
        }

        std.sort.insertion(File, compactable_files.items, {}, compareFilesLastModifiedTime);

        // TODO(Apaar): Traverse the files in "last modified time" descending so that
        // we see the most recent writes first, and then skip duplicate writes (e.g. a key
        // that already exists, don't bother copying it to the compacted file).
        //
        // This way we can avoid the key_to_set_op_range map altogether. We'll just need to
        // remember which keys have been deleted.

        for (compactable_files.items, 0..) |*file, compactable_file_index| {
            // HACK(Apaar): Mostly copypasta from `init`
            while (try readSegmentOpHeader(file)) |header| {
                switch (header) {
                    .set => |op| {
                        // op type + key len + value len
                        const header_size = 1 + 8 + 8;

                        // Start of the entire set operation
                        const op_start_pos = try file.getPos() - header_size;

                        // Read in the key
                        try temp_key_buf.resize(op.key_len);

                        _ = try file.readAll(temp_key_buf.items);

                        const set_op_range = SetOpRange{
                            .compactable_file_index = @intCast(compactable_file_index),
                            .pos = op_start_pos,
                            .size = header_size + op.key_len + op.value_len,
                            .value_offset_relative_to_pos = header_size + op.key_len,
                        };

                        // Skip over the value
                        _ = try file.seekBy(@intCast(op.value_len));

                        const entry = try key_to_set_op_range.getOrPutAdapted(
                            @as([]const u8, temp_key_buf.items),
                            key_to_set_op_range.ctx,
                        );

                        if (!entry.found_existing) {
                            // Copy it into our own non-temp buffer under our allocator
                            const key_buf = try self.allocator.alloc(u8, op.key_len);
                            @memcpy(key_buf, temp_key_buf.items);

                            entry.key_ptr.* = key_buf;
                        }

                        entry.value_ptr.* = set_op_range;
                    },

                    .del => |op| {
                        // FIXME(Apaar): What happens if there was a set operation that occurred
                        // after a previous del operation that we encountered? Should we order our
                        // traversal here by file mtime? That should ensure we have more recent ops
                        // afterwards.

                        // Read in the key
                        try temp_key_buf.resize(op.key_len);

                        _ = try file.readAll(temp_key_buf.items);

                        const removed = key_to_set_op_range.fetchRemove(temp_key_buf.items);

                        if (removed) |entry| {
                            self.allocator.free(entry.key);
                        }
                    },
                }
            }
        }

        // Now that we have the key and the latest ranges, we can copy them to the compacted file using
        // the copy range syscall.
        //
        // Note that we may not be able to finish copying everything in the time allotted.
        var key_to_set_op_range_iter = key_to_set_op_range.iterator();

        while (key_to_set_op_range_iter.next()) |entry| {
            const range = entry.value_ptr.*;

            var src_file = compactable_files.items[
                range.compactable_file_index
            ];

            const compacted_pos = try compacted_file.file.getEndPos();

            // TODO(Apaar): Handle failure to copy here?
            _ = try src_file.copyRangeAll(range.pos, compacted_file.file, compacted_pos, range.size);

            self.lock.lock();
            defer self.lock.unlock();

            // If there is no entry that means this key was deleted in the active file, just skip
            // adding it to the map.
            //
            // Yes, it's been written to the compacted file. No, I do not care. We want to avoid
            // locking in the scanning loop we do above so I'm fine with the overhead of the additional
            // unnecessary range (for now, until I benchmark, depends on workload? Can't think of
            // many delete-heavy workloads).
            var prev_entry = self.key_to_value_metadata.getEntry(entry.key_ptr.*) orelse continue;

            prev_entry.value_ptr.* = ValueMetadata{
                .offset = compacted_pos + range.value_offset_relative_to_pos,
                .len = range.size - range.value_offset_relative_to_pos,
                .file = compacted_file,
            };
        }

        // Unref all the segment files except for the last one (active)
        self.lock.lock();
        defer self.lock.unlock();

        for (0..self.segment_files.items.len - 1) |segment_file_index| {
            self.segment_files.items[segment_file_index].delete_on_close = true;
            self.segment_files.items[segment_file_index].unref();
        }

        var active_segment_file = self.segment_files.getLast();

        // Just need room for the compacted file and the active file
        try self.segment_files.resize(2);

        self.segment_files.items[0] = compacted_file;
        self.segment_files.items[1] = active_segment_file;
    }
};

export fn Store_create(dir_path: [*:0]const u8, max_segment_size: u64) ?*Store {
    const dir_path_slice = std.mem.span(dir_path);
    const store = std.heap.c_allocator.create(Store) catch return null;

    store.* = Store.init(std.heap.c_allocator, dir_path_slice, max_segment_size) catch return null;

    return store;
}

export fn Store_get(store: *Store, key: [*:0]const u8, createBufferFn: *const fn (u64) callconv(.C) [*c]u8) bool {
    const key_slice = std.mem.span(key);

    var proxy = store.get(key_slice) orelse return false;
    defer proxy.deinit();

    const buf = createBufferFn(proxy.len);

    _ = proxy.readInto(buf[0..proxy.len]) catch return false;

    return true;
}

const temp_prefix = "tmp/";
const TempPath = [temp_prefix.len + rand_path_len]u8;

fn tempPath() TempPath {
    var path: TempPath = undefined;
    @memcpy(path[0..temp_prefix.len], temp_prefix);

    var rand_path = randPath();
    @memcpy(path[temp_prefix.len..], &rand_path);

    return path;
}

const test_max_segment_size = 16;

test "set and get" {
    var dir_path = tempPath();

    var store = try Store.init(std.testing.allocator, &dir_path, test_max_segment_size);

    defer store.deinit();

    try store.setAllocKey("hello", "world");

    var proxy = store.get("hello").?;
    defer proxy.deinit();

    var value = try proxy.readAlloc(std.testing.allocator);
    defer std.testing.allocator.free(value);

    try std.testing.expectEqualStrings("world", value);
}

test "set multiple and get" {
    var dir_path = tempPath();

    var store = try Store.init(std.testing.allocator, &dir_path, test_max_segment_size);

    defer store.deinit();

    try store.setAllocKey("hello", "world");
    try store.setAllocKey("hello", "galaxy");
    try store.setAllocKey("hello", "universe");

    var proxy = store.get("hello").?;
    defer proxy.deinit();

    var value = try proxy.readAlloc(std.testing.allocator);
    defer std.testing.allocator.free(value);

    try std.testing.expectEqualStrings("universe", value);
}

test "set, close, open, and get" {
    var dir_path = tempPath();

    {
        var store = try Store.init(std.testing.allocator, &dir_path, test_max_segment_size);
        defer store.deinit();

        try store.setAllocKey("hello", "world");
    }

    var store = try Store.init(std.testing.allocator, &dir_path, test_max_segment_size);
    defer store.deinit();

    var proxy = store.get("hello").?;
    defer proxy.deinit();

    var value = try proxy.readAlloc(std.testing.allocator);
    defer std.testing.allocator.free(value);

    try std.testing.expectEqualStrings("world", value);
}

test "set, del, and get" {
    var dir_path = tempPath();

    var store = try Store.init(std.testing.allocator, &dir_path, test_max_segment_size);
    defer store.deinit();

    try store.setAllocKey("hello", "world");
    try std.testing.expect(try store.del("hello"));

    var proxy = store.get("hello");
    try std.testing.expectEqual(proxy, null);
}

test "set, del, close, open, and get" {
    var dir_path = tempPath();

    {
        var store = try Store.init(std.testing.allocator, &dir_path, test_max_segment_size);
        defer store.deinit();

        try store.setAllocKey("hello", "world");
        try std.testing.expect(try store.del("hello"));
    }

    var store = try Store.init(std.testing.allocator, &dir_path, test_max_segment_size);
    defer store.deinit();

    var proxy = store.get("hello");
    try std.testing.expectEqual(@as(?ValueProxy, null), proxy);
}

test "set, compact, and get" {
    var dir_path = tempPath();

    var store = try Store.init(std.testing.allocator, &dir_path, test_max_segment_size);

    defer store.deinit();

    try store.setAllocKey("hello", "world");
    try store.setAllocKey("hello", "universe");
    try store.setAllocKey("goodbye", "universe");

    try store.compactForMillis(10);

    var proxy = store.get("hello").?;
    defer proxy.deinit();

    var value = try proxy.readAlloc(std.testing.allocator);
    defer std.testing.allocator.free(value);

    try std.testing.expectEqualStrings("universe", value);
}

test "set, get, compact, and retrieve from previous get" {
    var dir_path = tempPath();

    var store = try Store.init(std.testing.allocator, &dir_path, test_max_segment_size);

    defer store.deinit();

    try store.setAllocKey("hello", "world");

    var proxy = store.get("hello").?;
    defer proxy.deinit();

    try store.setAllocKey("hello", "universe");

    try store.compactForMillis(10);

    var value = try proxy.readAlloc(std.testing.allocator);
    defer std.testing.allocator.free(value);

    try std.testing.expectEqualStrings("world", value);
}
