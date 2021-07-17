import time


class Timer:

    def __init__(self):
        self.t1 = None
        self.t2 = None

    def __enter__(self):
        self.t1 = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.t2 = time.time()
        return False

    @property
    def elapsed(self):
        return (self.t2 - self.t1) * 1000
