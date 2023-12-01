"""
Wraps the teardrop-store library written in Zig using ctypes.
"""
from typing import Optional

import ctypes

# TODO(Apaar): Make this configurable
_lib = ctypes.CDLL("./libteardrop-store.so")

_err_buf_argtypes = [ctypes.c_uint64, ctypes.c_char_p]

_Store_create = _lib.Store_create
_Store_create.argtypes = [ctypes.c_char_p, ctypes.c_uint64, *_err_buf_argtypes]
_Store_create.restype = ctypes.c_void_p

_Store_destroy = _lib.Store_destroy
_Store_destroy.argtypes = [ctypes.c_void_p]
_Store_destroy.restype = None

_CREATE_BUFFER_FN = ctypes.CFUNCTYPE(ctypes.c_void_p, ctypes.c_uint64)

_Store_get = _lib.Store_get
_Store_get.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.c_char_p, _CREATE_BUFFER_FN, *_err_buf_argtypes]
_Store_get.restype = ctypes.c_bool

_Store_set = _lib.Store_set
_Store_set.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.c_char_p, ctypes.c_uint64, ctypes.c_char_p, *_err_buf_argtypes]
_Store_set.restype = ctypes.c_bool

_Store_del = _lib.Store_del
_Store_del.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.c_char_p, *_err_buf_argtypes]
_Store_del.restype = ctypes.c_bool

_Store_compactForMillis = _lib.Store_compactForMillis
_Store_compactForMillis.argtypes = [ctypes.c_void_p, ctypes.c_uint64, *_err_buf_argtypes]
_Store_compactForMillis.restype = ctypes.c_bool

class Store:
    def __init__(self, dir_path: str, max_segment_size: int):
        self._err_buf = ctypes.create_string_buffer(1024)

        self._store = _Store_create(
            bytes(dir_path, encoding="utf8"), ctypes.c_uint64(max_segment_size), len(self._err_buf), self._err_buf,
        )

        if not self._store:
            raise Exception(f'Failed to open teardrop store: {self._err_buf.value}')

    def _call_with_err_buf(self, fn, *args):
        self._err_buf[0] = 0
        res = fn(*args, len(self._err_buf), self._err_buf)

        if self._err_buf.value:
            raise Exception(f'{fn.__name__} failed: {self._err_buf.value.decode()}')

        return res

    def close(self):
        _Store_destroy(self._store)

    def get(self, key: bytes) -> Optional[bytearray]:
        buffer = None

        def create_buffer(len: int) -> ctypes.c_void_p:
            nonlocal buffer

            # We create this buffer once in python land and return it to C.
            # The C code writes to it, and we smuggle it out of this function
            # as a byte array without performing any additional copies.
            #
            # In fact, there are precisely zero unnecessary copies; we create the
            # buffer here and it is populated directly from the file on disk.
            buffer = ctypes.create_string_buffer(len)
            buffer_address = ctypes.addressof(buffer)

            return buffer_address

        create_buffer_fn = _CREATE_BUFFER_FN(create_buffer)

        if not self._call_with_err_buf(_Store_get, self._store, len(key), key, create_buffer_fn):
            return None

        # In theory, this shouldn't perform a copy
        return bytearray(buffer)

    def set(self, key: bytes, value: bytes):
        self._call_with_err_buf(_Store_set, self._store, len(key), key, len(value), value)

    def delete(self, key: bytes) -> bool:
        self._call_with_err_buf(_Store_del, self._store, len(key), key)

    def compact_for_millis(self, ms: int):
        self._call_with_err_buf(_Store_compactForMillis, self._store, ms)