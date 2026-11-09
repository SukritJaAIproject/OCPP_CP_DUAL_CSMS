"""Test only the routing layer; never import the hardware-facing CP module."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
