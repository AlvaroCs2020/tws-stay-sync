import ctypes
import threading
import sys
import time
import os

class Watchdog:
    def __init__(self, timeout, callback):
        self.timeout = timeout
        self.callback = callback
        self._timer = None
        self._lock = threading.Lock()
        self._stopped = False
        print("[WATCHDOG] Se inicio el watchdog.")

    def _run(self):
        with self._lock:
            if not self._stopped:
                print("[WATCHDOG] Timeout alcanzado. Ejecutando callback.")
                self.callback()

    def start(self):
        with self._lock:
            self._stopped = False
            self._reset_timer()

    def reset(self):
        with self._lock:
            if not self._stopped:
                self._reset_timer()

    def stop(self):
        with self._lock:
            self._stopped = True
            if self._timer:
                self._timer.cancel()

    def _reset_timer(self):
        if self._timer:
            self._timer.cancel()
        self._timer = threading.Timer(self.timeout, self._run)
        self._timer.start()
        print("[WATCHDOG] Reset del watchdog (OK)")
class WatchdogTimeout(Exception):
    """Excepción lanzada cuando se dispara el watchdog"""
    pass
def raise_in_main_thread(exc_type):
    main_thread = threading.main_thread()
    for thread_id, frame in sys._current_frames().items():
        if threading._active.get(thread_id) is main_thread:
            res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_long(thread_id),
                ctypes.py_object(exc_type)
            )
            if res == 0:
                raise ValueError("Thread ID not found")
            elif res > 1:
                # Reset the exception if it was set more than once
                ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, None)
                raise SystemError("PyThreadState_SetAsyncExc failed")
            return