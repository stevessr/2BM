#!/usr/bin/env python3
"""Generate the Matrix AS registration using the same env secrets as 2BM."""

import argparse
import os
import re
from pathlib import Path


def load_env(path: Path) -> dict[str, str]:
    data = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            data[key.strip()] = value.strip().strip('"').strip("'")
    data.update(os.environ)
    return data


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=Path(".env"))
    parser.add_argument("--output", type=Path, default=Path("registration.yaml"))
    parser.add_argument("--url", required=True, help="URL homeserver can reach")
    args = parser.parse_args()
    env = load_env(args.env)
    required = ["BRIDGE_MATRIX_AS_TOKEN", "BRIDGE_MATRIX_HS_TOKEN", "BRIDGE_MATRIX_SERVER_NAME"]
    for name in required:
        if not env.get(name) or env[name].startswith("REPLACE_"):
            parser.error(f"set {name} first")
    if not args.url.startswith(("http://", "https://")):
        parser.error("--url must be an HTTP(S) URL")
    if any(c in args.url for c in "\r\n\t\"' "):
        parser.error("invalid characters in URL")
    server_name = env["BRIDGE_MATRIX_SERVER_NAME"]
    if not re.fullmatch(r"[A-Za-z0-9.\-:]+", server_name):
        parser.error("invalid server name")
    server = re.escape(server_name)
    as_token = env["BRIDGE_MATRIX_AS_TOKEN"]
    hs_token = env["BRIDGE_MATRIX_HS_TOKEN"]
    if not re.fullmatch(r"[A-Za-z0-9_-]{24,}", as_token) or not re.fullmatch(
        r"[A-Za-z0-9_-]{24,}", hs_token
    ):
        parser.error("tokens must be 24+ alphanumeric/underscore/dash characters")
    content = (
        "id: 2bm\n"
        f"url: '{args.url}'\n"
        f"as_token: '{as_token}'\n"
        f"hs_token: '{hs_token}'\n"
        "sender_localpart: _2bm\n"
        "rate_limited: false\n"
        "namespaces:\n"
        "  users:\n"
        "    - exclusive: true\n"
        f"      regex: '^@_2bm:{server}$'\n"
        "    - exclusive: true\n"
        f"      regex: '^@_2bm_p_[0-9a-f]{{32}}:{server}$'\n"
        "  aliases:\n"
        "    - exclusive: true\n"
        f"      regex: '^#_2bm_r_[0-9a-f]{{32}}:{server}$'\n"
        "  rooms: []\n"
    )
    args.output.write_text(content, encoding="utf-8")
    args.output.chmod(0o600)
    print(f"Wrote {args.output} (mode 0600); register with your Matrix homeserver")


if __name__ == "__main__":
    main()
