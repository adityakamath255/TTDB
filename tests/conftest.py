import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

SEG = 64 * 1024 * 1024
SEG_TINY = 256
SAVE_EVENTS = 1_000_000
SAVE_SECONDS = 5.0
FSYNC = "none"
