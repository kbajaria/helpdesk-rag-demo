#!/usr/bin/env python3
import argparse, json, subprocess, sys, time
import google.auth
from google.auth.transport.requests import AuthorizedSession

def j(x): return json.dumps(x, indent=2, ensure_ascii=False)

def run(cmd):
    return subprocess.run(cmd, text=True, capture_output=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--location", required=True)
    ap.add_argument("--corpus", required=True)
    ap.add_argument("--gcs-uri", required=True)
    ap.add_argument("--chunk-size", type=int, default=512)
    ap.add_argument("--chunk-overlap", type=int, default=100)
    ap.add_argument("--poll-seconds", type=int, default=60)
    ap.add_argument("--timeout-minutes", type=int, default=90)
    args = ap.parse_args()

    if not args.gcs_uri.startswith("gs://"):
        print("gcs-uri must start with gs://", file=sys.stderr)
        return 2
    bucket = args.gcs_uri[5:].split("/", 1)[0]
    sink = f"gs://{bucket}/import_results/chunked_{int(time.time())}/"

    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    session = AuthorizedSession(creds)

    base = f"https://{args.location}-aiplatform.googleapis.com/v1beta1"
    import_url = f"{base}/{args.corpus}/ragFiles:import"

    payload = {
        "importRagFilesConfig": {
            "gcsSource": {"uris": [args.gcs_uri]},
            "ragFileTransformationConfig": {
                "ragFileChunkingConfig": {
                    "fixedLengthChunking": {
                        "chunkSize": args.chunk_size,
                        "chunkOverlap": args.chunk_overlap,
                    }
                }
            },
            "importResultGcsSink": {"outputUriPrefix": sink},
        }
    }

    r = session.post(import_url, json=payload, timeout=60)
    if r.status_code >= 400:
        print("Complete")
        print("Result: FAILED (import start HTTP error)")
        print(r.text)
        return 1

    op_name = r.json().get("name")
    if not op_name:
        print("Complete")
        print("Result: FAILED (no operation name)")
        print(j(r.json()))
        return 1

    print(f"SINK={sink}")
    print(f"OP={op_name}")

    op_url = f"{base}/{op_name}"
    deadline = time.time() + args.timeout_minutes * 60

    while time.time() < deadline:
        op = session.get(op_url, timeout=60).json()
        if not op.get("done", False):
            print("Not Complete")
            time.sleep(args.poll_seconds)
            continue

        print("Complete")

        if op.get("error"):
            print("Result: FAILED (operation error)")
            print(j(op["error"]))
        else:
            resp = op.get("response") or {}
            md = op.get("metadata") or {}
            pf = (md.get("genericMetadata") or {}).get("partialFailures") or []
            failed = int(resp.get("failedRagFilesCount", "0"))
            if failed > 0 or pf:
                print("Result: FAILED (import reported failures)")
                print("Operation response:")
                print(j(resp))
                if pf:
                    print("Partial failures:")
                    print(j(pf))
            else:
                print("Result: SUCCESS (import completed)")
                print(j(resp))

        print("\n--- Import result sink listing ---")
        ls = run(["gsutil", "ls", f"{sink}**"])
        print((ls.stdout or ls.stderr).strip())

        print("\n--- First 200 lines of sink output ---")
        cat = run(["bash", "-lc", f"gsutil cat '{sink}'* 2>/dev/null | head -200"])
        out = (cat.stdout or "").strip()
        print(out if out else "(No sink output)")

        # return nonzero if failed
        return 0 if (op.get("error") is None and int((op.get("response") or {}).get("failedRagFilesCount","0")) == 0) else 1

    print("Complete")
    print("Result: FAILED (timed out)")
    return 1

if __name__ == "__main__":
    raise SystemExit(main())
