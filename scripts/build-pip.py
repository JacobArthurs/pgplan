#!/usr/bin/env python3
"""
Build platform-specific wheels for pgplan.

Usage:
    python scripts/build-pip.py <version> <binary_path> <platform_tag>

Example:
    python scripts/build-pip.py 0.1.0 dist/pgplan-linux-amd64 manylinux_2_17_x86_64.manylinux2014_x86_64
    python scripts/build-pip.py 0.1.0 dist/pgplan-darwin-arm64 macosx_11_0_arm64
    python scripts/build-pip.py 0.1.0 dist/pgplan-windows-amd64.exe win_amd64

Output wheels are written to dist/
"""

import hashlib
import stat
import sys
import zipfile
from base64 import urlsafe_b64encode
from pathlib import Path


def sha256_digest(data: bytes) -> str:
    h = hashlib.sha256(data)
    return "sha256=" + urlsafe_b64encode(h.digest()).decode("ascii").rstrip("=")


def build_wheel(version: str, binary_path: str, platform_tag: str):
    dist_dir = Path("dist")
    dist_dir.mkdir(exist_ok=True)

    binary_path = Path(binary_path)
    binary_data = binary_path.read_bytes()
    is_windows = platform_tag.startswith("win")
    bin_name = "pgplan.exe" if is_windows else "pgplan"

    name = "pgplan"
    tag = f"py3-none-{platform_tag}"
    wheel_name = f"{name}-{version}-{tag}.whl"
    dist_info = f"{name}-{version}.dist-info"

    # Resolve paths relative to this script's location
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent
    init_data = (repo_root / "pip" / "pgplan" / "__init__.py").read_bytes()
    readme = (repo_root / "README.md").read_text(encoding="utf-8")

    metadata = f"""\
Metadata-Version: 2.1
Name: {name}
Version: {version}
Summary: Compare and analyze PostgreSQL EXPLAIN plans from the CLI
Author: Jacob Arthurs
License: MIT
Keywords: postgresql,query-plan,explain,database,optimization
Classifier: Development Status :: 3 - Alpha
Classifier: Environment :: Console
Classifier: Intended Audience :: Developers
Classifier: License :: OSI Approved :: MIT License
Classifier: Programming Language :: Other
Classifier: Topic :: Database
Requires-Python: >=3.8
Project-URL: Homepage, https://github.com/JacobArthurs/pgplan
Project-URL: Repository, https://github.com/JacobArthurs/pgplan
Project-URL: Issues, https://github.com/JacobArthurs/pgplan/issues
Description-Content-Type: text/markdown

{readme}
""".encode()

    wheel_meta = f"""\
Wheel-Version: 1.0
Generator: pgplan-build
Root-Is-Purelib: false
Tag: {tag}
""".encode()

    entry_points = b"""\
[console_scripts]
pgplan = pgplan:main
"""

    records = []

    def record_entry(path: str, data: bytes):
        records.append(f"{path},{sha256_digest(data)},{len(data)}")

    wheel_path = dist_dir / wheel_name
    with zipfile.ZipFile(wheel_path, "w", zipfile.ZIP_DEFLATED) as whl:
        whl.writestr(f"{name}/__init__.py", init_data)
        record_entry(f"{name}/__init__.py", init_data)

        # Binary - set executable permission via external_attr
        bin_path = f"{name}/bin/{bin_name}"
        info = zipfile.ZipInfo(bin_path)
        if not is_windows:
            info.external_attr = (stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH) << 16
        info.compress_type = zipfile.ZIP_DEFLATED
        whl.writestr(info, binary_data)
        record_entry(bin_path, binary_data)

        whl.writestr(f"{dist_info}/METADATA", metadata)
        record_entry(f"{dist_info}/METADATA", metadata)

        whl.writestr(f"{dist_info}/WHEEL", wheel_meta)
        record_entry(f"{dist_info}/WHEEL", wheel_meta)

        whl.writestr(f"{dist_info}/entry_points.txt", entry_points)
        record_entry(f"{dist_info}/entry_points.txt", entry_points)

        # RECORD itself has no hash
        records.append(f"{dist_info}/RECORD,,")
        record_content = "\n".join(records) + "\n"
        whl.writestr(f"{dist_info}/RECORD", record_content)

    print(f"Built: {wheel_path} ({wheel_path.stat().st_size} bytes)")
    return wheel_path


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(__doc__)
        sys.exit(1)

    build_wheel(sys.argv[1], sys.argv[2], sys.argv[3])