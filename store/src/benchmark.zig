const std = @import("std");
const Store = @import("main.zig").Store;

const GPA = std.heap.GeneralPurposeAllocator(.{});

fn timeInMillis() isize {
    var tv: std.os.timeval = undefined;
    std.os.gettimeofday(&tv, null);

    return tv.tv_sec * 1000 + @divTrunc(tv.tv_usec, 1000);
}

pub fn main() !void {
    var gpa = GPA{};
    const allocator = gpa.allocator();

    var store = try Store.init(allocator, "tmp/benchmark", 1024 * 1024);
    defer store.deinit();

    const start_time = timeInMillis();

    const RUNS = 10000;

    for (0..RUNS) |_| {
        try store.setAllocKey("hello", "world");
        _ = try store.del("hello");
    }

    const end_time = timeInMillis();

    std.debug.print("Took {}ms to run {} ops\n", .{ end_time - start_time, RUNS * 2 });
}
