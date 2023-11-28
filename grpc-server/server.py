import sys

try:
    from . import store
except:
    import store

if __name__ == '__main__':
    my_store = store.Store(sys.argv[1], 1024)
    print(my_store.get(b'hello'))
