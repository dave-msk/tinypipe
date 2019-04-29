from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import queue

from tinypipe.blocks import base_block
from tinypipe.utils import general as gen_utils


class Pipe(base_block.Block):
  def __init__(self):
    super(Pipe, self).__init__()
    self._qin = None
    self._qout = None

  def _build_pipe(self):
    pass

  def _build(self, qin: queue.Queue, qout: queue.Queue=None):
    # TODO: Validate inputs
    self._qin = qin
    self._qout = qout
    self._build_pipe()

  def wrap_up(self):
    super(Pipe, self).wrap_up()
    self.join()


class FetchRetryPipe(Pipe):
  def __init__(self,
               max_retry=None,
               fetch_time=None):
    super(FetchRetryPipe, self).__init__()
    self._max_retry = gen_utils.val_or_default(max_retry, 10)
    self._fetch_time = gen_utils.val_or_default(fetch_time, 0.1)
    self._retry_count = 0

  def _fetch_data(self):
    try:
      data = self._qin.get(timeout=self._fetch_time)
      self._retry_count = 0
      return data
    except queue.Empty:
      if not self._running:
        self._retry_count += 1
      raise

  def _ready_to_terminate(self):
    return self._retry_count >= self._max_retry
