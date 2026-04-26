import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from timedb.db import DEFAULT_SEGMENT_BYTES

SEG = DEFAULT_SEGMENT_BYTES
SEG_TINY = 256
