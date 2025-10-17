from typing import Callable
from iotanalyzer.models import Recording


Filter = Callable[[Recording], bool]
