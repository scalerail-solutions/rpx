#!/usr/bin/env bash
set -euo pipefail

next_version="${1:?usage: scripts/bump-version.sh <semver>}"

if [[ ! "$next_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "version must be a plain semver like 1.2.3" >&2
    exit 1
fi

python3 - "$next_version" <<'PY'
from pathlib import Path
import re
import sys

next_version = sys.argv[1]

cargo_toml = Path("Cargo.toml")
toml_text = cargo_toml.read_text()
toml_text, changed = re.subn(
    r'(\[package\]\nname = "rpx"\nversion = ")[^"]+("\nedition = "2024")',
    rf'\g<1>{next_version}\g<2>',
    toml_text,
    count=1,
)
if changed != 1:
    raise SystemExit("failed to update Cargo.toml version")
cargo_toml.write_text(toml_text)

cargo_lock = Path("Cargo.lock")
lock_text = cargo_lock.read_text()
lock_text, changed = re.subn(
    r'(\[\[package\]\]\nname = "rpx"\nversion = ")[^"]+("\n)',
    rf'\g<1>{next_version}\g<2>',
    lock_text,
    count=1,
)
if changed != 1:
    raise SystemExit("failed to update Cargo.lock version")
cargo_lock.write_text(lock_text)
PY
