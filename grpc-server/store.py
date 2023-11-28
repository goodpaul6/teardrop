"""
Wraps the teardrop-store library written in Zig using ctypes.
"""
from typing import Optional

import ctypes

# TODO(Apaar): Make this configurable
_lib = ctypes.CDLL("./libteardrop-store.so")

_Store_create = _lib.Store_create
_Store_create.argtypes = [ctypes.c_char_p, ctypes.c_uint64]
_Store_create.restype = ctypes.c_void_p

_CREATE_BUFFER_FN = ctypes.CFUNCTYPE(ctypes.c_void_p, ctypes.c_uint64)

_Store_get = _lib.Store_get
_Store_get.argtypes = [ctypes.c_void_p, ctypes.c_char_p, _CREATE_BUFFER_FN]
_Store_get.restype = ctypes.c_bool


class Store:
    def __init__(self, dir_path: str, max_segment_size: int):
        self._store = _Store_create(
            bytes(dir_path, encoding="utf8"), ctypes.c_uint64(max_segment_size)
        )

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

        if not _Store_get(self._store, key, create_buffer_fn):
            return None

        # In theory, this shouldn't perform a copy
        return bytearray(buffer)
