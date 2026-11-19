#!/usr/bin/env python3
"""
Upload the bundle(s) in a directory to an Internet Archive item.

Uploads every si_*.zip plus its si_*.txt manifest from --dir into the item
--identifier, creating the item on first use. Files already on the item with
the same checksum are skipped, so re-running a day is safe.

Credentials come from the IA_ACCESS_KEY and IA_SECRET_KEY environment
variables (get them at https://archive.org/account/s3.php).

Exit codes:
    0  everything uploaded (or already present)
    1  missing credentials, nothing to upload, or an upload failed
"""

import argparse
import os
import sys
from pathlib import Path

from internetarchive import get_item


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dir", default="downloads", help="Directory containing si_*.zip / si_*.txt")
    parser.add_argument("--identifier", required=True, help="Internet Archive item identifier")
    parser.add_argument("--title", required=True, help="Item title (used only when creating the item)")
    parser.add_argument("--collection", default="opensource_media", help="Collection (creation only)")
    parser.add_argument("--mediatype", default="texts", help="Media type (creation only)")
    parser.add_argument("--source", default="", help="Source URL (creation only)")
    parser.add_argument("--retries", type=int, default=5, help="Retries per file on server errors")
    args = parser.parse_args()

    access_key = os.environ.get("IA_ACCESS_KEY")
    secret_key = os.environ.get("IA_SECRET_KEY")
    if not access_key or not secret_key:
        print("[fail] IA_ACCESS_KEY / IA_SECRET_KEY are not set")
        raise SystemExit(1)

    directory = Path(args.dir)
    files = sorted(directory.glob("si_*.zip")) + sorted(directory.glob("si_*.txt"))
    if not files:
        print(f"[fail] no si_*.zip / si_*.txt files in {directory}")
        raise SystemExit(1)

    item = get_item(args.identifier, config={"s3": {"access": access_key, "secret": secret_key}})

    # Metadata only when creating: mediatype can't be changed later, and we
    # don't want daily runs rewriting the item's description/title.
    metadata = None
    if not item.exists:
        metadata = {
            "title": args.title,
            "collection": args.collection,
            "mediatype": args.mediatype,
        }
        if args.source:
            metadata["source"] = args.source
        print(f"Item {args.identifier} does not exist yet; creating it.")
    else:
        print(f"Adding to existing item {args.identifier}.")

    for f in files:
        print(f"  uploading {f.name} ({f.stat().st_size / (1024 * 1024):.1f} MiB)")

    try:
        responses = item.upload(
            [str(f) for f in files],
            metadata=metadata,
            access_key=access_key,
            secret_key=secret_key,
            checksum=True,           # skip files already on IA with the same MD5
            verify=True,             # check the uploaded size/MD5 after upload
            retries=args.retries,
            retries_sleep=30,
            verbose=True,
            validate_identifier=True,
        )
    except Exception as e:  # network errors, auth errors, bad identifier...
        print(f"[fail] upload raised {type(e).__name__}: {e}")
        raise SystemExit(1)

    # A skipped (already present) file may come back as None; anything else
    # must be HTTP 200.
    bad = [(f.name, r.status_code) for f, r in zip(files, responses)
           if r is not None and getattr(r, "status_code", 200) != 200]
    if bad:
        for name, status in bad:
            print(f"[fail] {name}: HTTP {status}")
        raise SystemExit(1)

    print(f"Done. Uploaded {len(files)} file(s) to https://archive.org/details/{args.identifier}")


if __name__ == "__main__":
    sys.exit(main())
