"""Sample script for Jenkins runs.

This script prints a message, computes a simple sum, and writes output.txt.
Exit code 0 on success.
"""

import sys


def main():
    print("Hello from Jenkins sample script")
    total = sum(range(1, 6))
    print(f"Sum 1..5 = {total}")
    with open("output.txt", "w", encoding="utf-8") as f:
        f.write(f"Sum={total}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
