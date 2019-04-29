from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import queue

from tinypipe.blocks.pipe import base as base_pipe


class BatchPipe(base_pipe.FetchRetryPipe):
  def __init__(self,
               batch_size,
               max_retry=None,
               fetch_time=None):
    super(BatchPipe, self).__init__(
        max_retry=max_retry, fetch_time=fetch_time)
    self._batch_size = batch_size
    self._batched = []

  def _run(self):
    try:
      data = self._fetch_data()
      self._batched.append(data)
    except queue.Empty:
      pass
    if ((len(self._batched) >= self._batch_size) or
        (self._batched and not self._running and
         self._retry_count >= self._max_retry // 2)):
      self._qout.put(list(self._batched))
      [self._qin.task_done() for _ in range(len(self._batched))]
      self._batched.clear()