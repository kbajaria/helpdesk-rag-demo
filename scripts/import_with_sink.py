#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone

import google.auth
from google.auth.transport.requests import AuthorizedSession

def j(x): return json.dumps(x, indent=2, ensure_ascii=False)

def run(cmd):
    return subprocess.run(cmd, text=True, capture_output=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--location", required=True)
    ap.add_argument("--corpus", required=True)          # projects/.../locations/.../ragCorpora/...
    ap.add_argument("--gcs-uri", required=True)         # gs://bucket/path.ndjson
    ap.add_argument("--sink-prefix", default=None)      # gs://bucket/prefix/  (optional)
    ap.add_argument("--poll-seconds", type=int, default=60)
    ap.add_argument("--timeout-minutes", type=int, default=60)
    args = ap.parse_args()

    if not args.gcs_uri.startswith("gs://"):
        print("gcs-uri must start with gs://", file=sys.stderr)
        return 2

    # Derive bucket for default sink
    parts = args.gcs_uri[5:].split("/", 1)
    bucket = parts[0]

    if args.sink_prefix:
        sink = args.sink_prefix
        if not sink.startswith("gs://"):
            print("sink-prefix must start with gs://", file=sys.stderr)
            return 2
        if not sink.endswith("/"):
            sink += "/"
    else:
        ts = int(time.time())
        sink = f"gs://{bucket}/import_results/vertex_{ts}/"

    # Auth session
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    session = AuthorizedSession(creds)

    base = f"https://{args.location}-aiplatform.googleapis.com/v1beta1"
    import_url = f"{base}/{args.corpus}/ragFiles:import"

    payload = {
        "importRagFilesConfig": {
            "gcsSource": {"uris": [args.gcs_uri]},
            "importResultGcsSink": {"outputUriPrefix": sink},
        }
    }

    # Start import
    resp = session.post(import_url, json=payload, timeout=60)
    if resp.status_code >= 400:
        print("Complete")
        print("Result: FAILED (import start HTTP error)")
        print(resp.text)
        return 1

    op = resp.json()
    op_name = op.get("name")
    if not op_name:
        print("Complete")
        print("Result: FAILED (no operation name returned)")
        print(j(op))
        return 1

    print(f"SINK={sink}")
    print(f"OP={op_name}")

    # Poll operation
    deadline = time.time() + args.timeout_minutes * 60
    op_url = f"{base}/{op_name}"

    while time.time() < deadline:
        r = session.get(op_url, timeout=60)
        if r.status_code >= 400:
            print("Complete")
            print("Result: FAILED (operation GET HTTP error)")
            print(r.text)
            return 1

        obj = r.json()
        done = bool(obj.get("done", False))
        if not done:
            print("Not Complete")
            time.sleep(args.poll_seconds)
            continue

        print("Complete")

        if obj.get("error"):
            print("Result: FAILED (operation error)")
            print(j(obj["error"]))
        else:
            response = obj.get("response") or {}
            md = obj.get("metadata") or {}
            pf = (md.get("genericMetadata") or {}).get("partialFailures") or []

            failed_count = int((response.get("failedRagFilesCount") or "0"))
            if failed_count > 0 or pf:
                print("Result: FAILED (import reported failures)")
                print("Operation response:")
                print(j(response))
                if pf:
                    print("Partial failures:")
                    print(j(pf))
            else:
                print("Result: SUCCESS (import operation completed with no reported failures)")
                print(j(response))

        # Print sink contents (best effort)
        print("\n--- Import result sink listing ---")
        ls = run(["gsutil", "ls", f"{sink}**"])
        if ls.returncode != 0:
            print(ls.stderr.strip() or ls.stdout.strip())
            return 1

        print(ls.stdout.strip())

        print("\n--- First 200 lines of sink output ---")
        cat = run(["bash", "-lc", f"gsutil cat '{sink}'* 2>/dev/null | head -200"])
        out = (cat.stdout or "").strip()
        if out:
            print(out)
        else:
            print("(No readable sink output yet)")

        # If we got here and it wasn’t a clean success, return non-zero
        return 0 if (obj.get("error") is None and int((obj.get("response") or {}).get("failedRagFilesCount","0")) == 0) else 1

    print("Complete")
    print(f"Result: FAILED (timed out after {args.timeout_minutes} minutes)")
    return 1

if __name__ == "__main__":
    raise SystemExit(main())
