import sys
import time

try:
    from . import store
except:
    import store

if __name__ == '__main__':
    my_store = store.Store(sys.argv[1], 1024 * 1024)

    key = b'goodbye'
    value = b'example'

    start_time = time.time()

    for i in range(3000):
        my_store.set(key, value)
        my_store.delete(key)
        my_store.set(key, value)

    print(f'Took {time.time()-start_time=}s to do 9K ops')

    print(my_store.get(key))
 
    my_store.compact_for_millis(10)    

    print(my_store.get(key))

    my_store.close()