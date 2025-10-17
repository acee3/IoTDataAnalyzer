from typing import Callable
from models import Recording


Filter = Callable[[Recording], bool]
