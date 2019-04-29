from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

from tinypipe.blocks.pipe import base as base_pipe
from tinypipe.utils import general as gen_utils


class ParallelPipe(base_pipe.Pipe):
  def __init__(self,
               pipe_init_fn,
               num_parallel_pipes=None,
               cooldown_secs=None):
    # TODO: Add validation logic:
    # 1. `pipe_init_fn`: Function that takes no argument, returns a new `Pipe`
    # 2. `num_pipes`: Positive integer
    # 3. `cooldown_secs`: positive float

    super(ParallelPipe, self).__init__()
    self._pipes = []
    self._pipe_init_fn = pipe_init_fn
    self._num_parallel_pipes = gen_utils.val_or_default(num_parallel_pipes, 1)
    self._cooldown_secs = gen_utils.val_or_default(cooldown_secs, 0.1)

  def _run(self):
    time.sleep(self._cooldown_secs)

  def _ready_to_terminate(self):
    return not any(p.is_alive() for p in self._pipes)

  def _build_pipe(self):
    # TODO: Add validation logic
    for _ in range(self._num_parallel_pipes):
      p = self._pipe_init_fn()
      p.build(self._qin, self._qout)
      self._pipes.append(p)

  def wrap_up(self):
    [p.wrap_up() for p in self._pipes]
    super(ParallelPipe, self).wrap_up()
