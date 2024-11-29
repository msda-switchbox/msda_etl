"""get memory usage"""

import gc
import os

import psutil


def set_gc_threshold_mult(multiplier: int = 3):
    """
    set the garbage collection thresholds to a given multiple of the current
    thresholds; giving 0 as a multiplier disables garbage collection
    """
    # see: https://docs.python.org/3/library/gc.html#gc.set_threshold
    gc.set_threshold(*[threshold * multiplier for threshold in gc.get_threshold()])


def get_memory_use():
    """
    Utility function to get the current memory usage of
    the etl process.
    """
    process = psutil.Process(os.getpid())
    return process.memory_info().rss  # in bytes
