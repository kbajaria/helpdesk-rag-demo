import os
from typing import List, Dict, Any

import google.auth
from google.auth.transport.requests import AuthorizedSession
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ---- Config ----
PROJECT_ID = os.getenv("PROJECT_ID", "helpdesk-rag")
RAG_LOCATION = os.getenv("RAG_LOCATION", "europe-west4")

# Your corpus (already created + active)
RAG_CORPUS = os.getenv(
    "RAG_CORPUS",
    "projects/1088823216033/locations/europe-west4/ragCorpora/7991637538768945152"
)

# Gemini model (use global unless you prefer a specific region)
GEMINI_MODEL = os.getenv(
    "GEMINI_MODEL",
    f"projects/{PROJECT_ID}/locations/global/publishers/google/models/gemini-2.5-flash"
)

TOP_K = int(os.getenv("TOP_K", "12"))

# ---- Auth session ----
creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
session = AuthorizedSession(creds)

app = FastAPI(title="Helpdesk RAG Demo")

# If you serve UI from same app, CORS isn’t necessary, but it’s convenient for local dev.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class AskReq(BaseModel):
    question: str

def retrieve_contexts(question: str, top_k: int = 4) -> List[Dict[str, Any]]:
    """
    Calls locations.retrieveContexts:
      POST https://.../v1beta1/{parent}:retrieveContexts
    Request body has fields: query + vertexRagStore (data source). :contentReference[oaicite:1]{index=1}
    """
    parent = f"projects/{PROJECT_ID}/locations/{RAG_LOCATION}"
    url = f"https://{RAG_LOCATION}-aiplatform.googleapis.com/v1beta1/{parent}:retrieveContexts"

    payload = {
        "query": {
            # similarityTopK is deprecated but works and is simple. :contentReference[oaicite:2]{index=2}
            "text": question,
            "similarityTopK": top_k,
        },
        "vertexRagStore": {
            "ragResources": [
                {"ragCorpus": RAG_CORPUS}
            ]
        },
    }

    r = session.post(url, json=payload, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"retrieveContexts failed {r.status_code}: {r.text}")

    data = r.json()
    return (data.get("contexts") or {}).get("contexts") or []
    
# Comment

def generate_answer(question: str, contexts: List[Dict[str, Any]]) -> str:
    """
    Calls publishers.models.generateContent:
      POST https://aiplatform.googleapis.com/v1/{model}:generateContent :contentReference[oaicite:3]{index=3}
    """
    # Build a compact context block
    ctx_lines = []
    sources = []
    for i, c in enumerate(contexts, 1):
        src = c.get("sourceUri") or c.get("sourceDisplayName") or "unknown"
        txt = (c.get("text") or "").strip()
        if not txt:
            continue
        sources.append(src)
        ctx_lines.append(f"[{i}] Source: {src}\n{txt}")

    context_block = "\n\n".join(ctx_lines) if ctx_lines else "(No relevant context found.)"

    system = """
        You are a helpdesk intelligence assistant. Write a clear, human-readable report.

        Rules:
        - Use the provided CONTEXT as evidence and cite with [1], [2], etc.
        - If something is not supported by context, label it as "Hypothesis" or "General guidance".
        - Prefer specific phrases/errors/URLs found in the context.

        Output format (use these headings):
        1) Executive summary (2-3 bullets)
        2) What environments are mentioned (UAT / Prod / other)
        3) Common error patterns (grouped by environment)
        4) Suggested triage questions (max 6)
        5) Suggested next actions (max 6)
        6) Evidence (list the citations with 1-line description)
        """

    prompt = f"QUESTION:\n{question}\n\nCONTEXT:\n{context_block}"

    url = f"https://aiplatform.googleapis.com/v1/{GEMINI_MODEL}:generateContent"
    payload = {
        "systemInstruction": {"role": "system", "parts": [{"text": system}]},
        "contents": [
            {"role": "user", "parts": [{"text": prompt}]}
        ],
        "generationConfig": {
            "temperature": 0.3,
            "maxOutputTokens": 4000,
        },
    }

    r = session.post(url, json=payload, timeout=120)
    if r.status_code >= 400:
        raise RuntimeError(f"generateContent failed {r.status_code}: {r.text}")

    data = r.json()
    # Typical response path: candidates[0].content.parts[0].text
    try:
        cand = (data.get("candidates") or [{}])[0]
        parts = (cand.get("content") or {}).get("parts") or []
        return "".join(p.get("text", "") for p in parts).strip() or str(data)

    except Exception:
        return str(data)

@app.get("/")
def home():
    return FileResponse("demo_app/static/index.html")

@app.post("/api/ask")
def ask(req: AskReq):
    try:
        contexts = retrieve_contexts(req.question, TOP_K)
        answer = generate_answer(req.question, contexts)
        # Return top contexts for demo transparency
        return JSONResponse({
            "answer": answer,
            "contexts": [
                {
                    "sourceUri": c.get("sourceUri"),
                    "score": c.get("score"),
                    "text": (c.get("text") or "")[:800]  # keep payload small
                }
                for c in contexts
            ]
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
