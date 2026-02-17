#!/usr/bin/env python3
import argparse
import json
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import google.auth
from google.auth.transport.requests import AuthorizedSession

def jdump(x: Any) -> str:
    return json.dumps(x, indent=2, ensure_ascii=False)

def exit_with_error(msg: str, details: Optional[Any] = None, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    if details is not None:
        print(jdump(details), file=sys.stderr)
    sys.exit(code)

def make_session() -> AuthorizedSession:
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return AuthorizedSession(creds)

def base_url(location: str) -> str:
    return f"https://{location}-aiplatform.googleapis.com/v1beta1"

def http_json(session: AuthorizedSession, method: str, url: str,
              payload: Optional[Dict[str, Any]] = None,
              params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    try:
        if method == "GET":
            resp = session.get(url, params=params, timeout=60)
        elif method == "POST":
            resp = session.post(url, json=payload, timeout=60)
        else:
            raise ValueError(f"Unsupported method: {method}")
    except Exception as e:
        exit_with_error(f"HTTP request failed ({method} {url}): {e}")

    if not (200 <= resp.status_code < 300):
        try:
            err = resp.json()
        except Exception:
            err = {"status_code": resp.status_code, "body": resp.text}
        exit_with_error(f"Request failed: {method} {url} (HTTP {resp.status_code})", err)

    try:
        return resp.json()
    except Exception:
        exit_with_error(f"Expected JSON response but got non-JSON from {method} {url}", {"body": resp.text})

def import_rag_files(session: AuthorizedSession, location: str, corpus_name: str, gcs_uris: List[str]) -> str:
    url = f"{base_url(location)}/{corpus_name}/ragFiles:import"
    body = {"importRagFilesConfig": {"gcsSource": {"uris": gcs_uris}}}
    op = http_json(session, "POST", url, payload=body)
    op_name = op.get("name")
    if not op_name:
        exit_with_error("Import returned success but missing Operation 'name'.", op)
    return op_name

def get_operation(session: AuthorizedSession, location: str, op_name: str) -> Dict[str, Any]:
    url = f"{base_url(location)}/{op_name}"
    return http_json(session, "GET", url)

def list_rag_files(session: AuthorizedSession, location: str, corpus_name: str) -> List[Dict[str, Any]]:
    url = f"{base_url(location)}/{corpus_name}/ragFiles"
    all_files: List[Dict[str, Any]] = []
    page_token = None
    while True:
        params = {"pageToken": page_token} if page_token else None
        resp = http_json(session, "GET", url, params=params)
        all_files.extend(resp.get("ragFiles", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return all_files

def get_rag_file(session: AuthorizedSession, location: str, rag_file_name: str) -> Dict[str, Any]:
    url = f"{base_url(location)}/{rag_file_name}"
    return http_json(session, "GET", url)

def rag_file_state(rf: Dict[str, Any]) -> Tuple[str, str]:
    fs = rf.get("fileStatus") or {}
    state = fs.get("state") or "STATE_UNSPECIFIED"
    err = fs.get("errorStatus") or ""
    return state, err

def extract_partial_failures(op: Dict[str, Any]) -> List[Dict[str, Any]]:
    md = op.get("metadata") or {}
    gm = md.get("genericMetadata") or {}
    return gm.get("partialFailures") or []

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--location", required=True)
    ap.add_argument("--corpus", required=True)
    ap.add_argument("--gcs-uri", required=True, action="append")
    ap.add_argument("--poll-seconds", type=int, default=60)
    ap.add_argument("--timeout-minutes", type=int, default=120)
    args = ap.parse_args()

    session = make_session()

    op_name = import_rag_files(session, args.location, args.corpus, args.gcs_uri)
    print(f"Import started. Operation: {op_name}")

    deadline = time.time() + (args.timeout_minutes * 60)

    while time.time() < deadline:
        op = get_operation(session, args.location, op_name)
        done = bool(op.get("done", False))
        if not done:
            print("Not Complete")
            time.sleep(args.poll_seconds)
            continue

        print("Complete")

        if op.get("error"):
            print("Result: FAILED (operation error)")
            print(jdump(op["error"]))
            sys.exit(1)

        resp = op.get("response") or {}
        failed_count = int(resp.get("failedRagFilesCount", "0"))
        partial_failures = extract_partial_failures(op)

        if failed_count > 0 or partial_failures:
            print("Result: FAILED (import reported failures)")
            print("Operation response:")
            print(jdump(resp))
            if partial_failures:
                print("Partial failures:")
                print(jdump(partial_failures))
            sys.exit(1)

        print("Result: SUCCESS (import operation completed with no reported failures).")
        break
    else:
        print("Complete")
        print(f"Result: FAILED (timed out after {args.timeout_minutes} minutes waiting for import operation).")
        sys.exit(1)

    # Wait for ragFiles to appear and become ACTIVE
    print("Waiting for RagFiles to appear...")
    rf_deadline = time.time() + (args.timeout_minutes * 60)
    names: List[str] = []
    while time.time() < rf_deadline:
        rag_files = list_rag_files(session, args.location, args.corpus)
        names = [rf.get("name") for rf in rag_files if rf.get("name")]
        if names:
            break
        print("Not Complete")
        time.sleep(args.poll_seconds)

    if not names:
        exit_with_error("Complete\nResult: FAILED (no RagFiles found in corpus after import).",
                        {"corpus": args.corpus})

    print(f"Found {len(names)} RagFile(s). Waiting for ACTIVE.")

    pending = set(names)
    while pending and time.time() < rf_deadline:
        for name in list(pending):
            rf = get_rag_file(session, args.location, name)
            state, err = rag_file_state(rf)
            if state == "ACTIVE":
                pending.remove(name)
            elif state == "ERROR":
                print("Complete")
                print("Result: FAILED (a RagFile entered ERROR state).")
                print(f"RagFile: {name}")
                print("errorStatus:")
                print(err or "(no errorStatus provided)")
                print("Full RagFile:")
                print(jdump(rf))
                sys.exit(1)
        if pending:
            print("Not Complete")
            time.sleep(args.poll_seconds)

    if pending:
        print("Complete")
        print(f"Result: FAILED (timed out waiting for RagFiles to become ACTIVE). Pending: {len(pending)}")
        for n in sorted(pending):
            print("-", n)
        sys.exit(1)

    print("Complete")
    print("Final Result: SUCCESS (all RagFiles are ACTIVE and indexed).")

if __name__ == "__main__":
    main()
