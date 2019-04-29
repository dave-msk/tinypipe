from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import queue

from tinypipe.blocks.pipe import base as pipe_base


class FunctionPipe(pipe_base.FetchRetryPipe):
  def __init__(self,
               fn,
               max_retry=None,
               fetch_time=None):
    super(FunctionPipe, self).__init__(
        max_retry=max_retry, fetch_time=fetch_time)
    self._fn = fn

  def _run(self):
    try:
      data = self._fetch_data()
      out = self._fn(data)
      if self._qout is not None:
        self._qout.put(out)
      self._qin.task_done()
    except queue.Empty:
      pass
