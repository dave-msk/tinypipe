from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import queue
import time

from tinypipe.blocks.pipe import base as base_pipe
from tinypipe.utils import general as gen_utils


class ChainedPipe(base_pipe.Pipe):
  def __init__(self,
               pipes,
               capacity=None,
               cooldown_secs=None):
    # TODO: Add validation logic:
    # 1. `pipes`: non-empty list of non-built `Pipe`s
    # 2. `capacity`: non-negative integer
    # 3. `cooldown_secs`: positive float
    super(ChainedPipe, self).__init__()
    self._pipes = pipes
    self._capacity = gen_utils.val_or_default(capacity, 0)
    self._cooldown_secs = gen_utils.val_or_default(cooldown_secs, 0.1)

  def _run(self):
    time.sleep(self._cooldown_secs)

  def _ready_to_terminate(self):
    return not any(p.is_alive() for p in self._pipes)

  def _build_pipe(self):
    qin = self._qin
    for i in range(len(self._pipes) - 1):
      p = self._pipes[i]
      qout = queue.Queue(maxsize=self._capacity)
      p.build(qin, qout)
      qin = qout
    self._pipes[-1].build(qin, self._qout)

  def wrap_up(self):
    [p.wrap_up() for p in self._pipes]
    super(ChainedPipe, self).wrap_up()
