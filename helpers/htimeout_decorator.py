import sys
import threading
import _thread


def timeout_handler(function_name):
    sys.stderr.flush()
    # Raise keyboard
    _thread.interrupt_main()


def exit_after(s: int):
    '''
    Decorator will exit process if function execution takes longer than s seconds
    '''
    def outer(fn):
        def inner(*args, **kwargs):
            timer = threading.Timer(s, timeout_handler, args=[fn.__name__])
            timer.start()
            try:
                result = fn(*args, **kwargs)
            finally:
                timer.cancel()
            return result
        return inner
    return outer
