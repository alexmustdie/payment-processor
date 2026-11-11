"""Generate a local key without displaying it or overwriting an existing .env."""

import os
import secrets
from pathlib import Path

target = Path(__file__).resolve().parents[1] / ".env"
try:
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
except FileExistsError:
    print(".env already exists; kept unchanged")
else:
    with os.fdopen(descriptor, "w") as stream:
        stream.write(f"API_KEY={secrets.token_urlsafe(32)}\n")
    print(".env created with a random API key")
