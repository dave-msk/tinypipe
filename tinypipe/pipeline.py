from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import queue
import threading

from tinypipe.blocks import pipe as pipe_lib


class Pipeline(object):
  def __init__(self, capacity=None, cooldown_secs=None):
    self._capacity = capacity
    self._cooldown_secs = cooldown_secs
    self._pipes = []

    self._lock = threading.Lock()
    self._chain = None
    self._qin = None
    self._built = False

  def append_pipe(self, pipe: pipe_lib.Pipe):
    # Do we need a lock here?
    self._pipes.append(pipe)

  def build(self):
    if self._built: return

    with self._lock:
      if self._built: return
      self._qin = queue.Queue()
      self._chain = pipe_lib.ChainedPipe(self._pipes,
                                         capacity=self._capacity,
                                         cooldown_secs=self._cooldown_secs)
      self._chain.build(self._qin, None)
      self._built = True

  def start(self):
    self._chain.start()

  def join(self):
    self._chain.join()
