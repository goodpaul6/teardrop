"""
Wraps the teardrop-store library written in Zig using ctypes.
"""

import ctypes

# TODO(Apaar): Make this configurable
_lib = ctypes.CDLL("./libteardrop-store.so")

class _CBuffer(ctypes.Structure):
    _fields_ = [
        ("ptr", ctypes.POINTER(ctypes.c_ubyte)),
        ("len", ctypes.c_uint64)
    ]

_Store_create = _lib.Store_create
_Store_create.argtypes = [ctypes.c_char_p, ctypes.c_uint64]
_Store_create.restype = ctypes.c_void_p

_Store_getAllocValue = _lib.Store_getAllocValue
_Store_getAllocValue.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
_Store_getAllocValue.restype = _CBuffer

_CBuffer_free = _lib.CBuffer_free
_CBuffer_free.argtypes = [ctypes.POINTER(_CBuffer)]
_CBuffer_free.restype = None

class Store:
    def __init__(self, dir_path: str, max_segment_size: int):
        self._store = _Store_create(bytes(dir_path, encoding='utf8'), ctypes.c_uint64(max_segment_size))

    def get(self, key: bytes) -> bytes:
        buf = _Store_getAllocValue(self._store, key)
        res = ctypes.string_at(buf.ptr, buf.len)
        _CBuffer_free(ctypes.byref(buf))

        return res