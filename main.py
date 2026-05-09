import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from web.app import start

if __name__ == "__main__":
    print("\n╔════════════════════════════════════════════╗")
    print("║   Smart Parking IoT Protocol Simulator     ║")
    print("╚════════════════════════════════════════════╝")
    print("  Dashboard →  http://localhost:8001")
    print("  Press Ctrl+C to quit\n")
    start()