import os
import sys
import subprocess


def _get_binary_path():
    """Resolve the pgplan binary bundled in this wheel."""
    bin_dir = os.path.join(os.path.dirname(__file__), "bin")
    if sys.platform == "win32":
        binary = os.path.join(bin_dir, "pgplan.exe")
    else:
        binary = os.path.join(bin_dir, "pgplan")

    if not os.path.isfile(binary):
        raise FileNotFoundError(
            f"pgplan binary not found at {binary}. "
            "Your platform may not be supported. "
            "See: https://github.com/JacobArthurs/pgplan"
        )
    return binary


def main():
    binary = _get_binary_path()
    try:
        result = subprocess.run(
            [binary] + sys.argv[1:],
            stdin=sys.stdin,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        raise SystemExit(result.returncode)
    except KeyboardInterrupt:
        raise SystemExit(130)