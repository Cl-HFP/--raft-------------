import threading
class RWLock(object):
    def __init__(self):
        self.rlock = threading.Lock()
        self.wlock = threading.Lock()
        self.reader = 0
    #写者锁
    def write_acquire(self):
        self.wlock.acquire()

    def write_release(self):
        self.wlock.release()
    #读者锁
    def read_acquire(self):
        self.rlock.acquire()
        self.reader += 1
        if self.reader == 1:
            self.wlock.acquire()
        self.rlock.release()

    def read_release(self):
        self.rlock.acquire()
        self.reader -= 1
        if self.reader == 0:
            self.wlock.release()
        self.rlock.release()
