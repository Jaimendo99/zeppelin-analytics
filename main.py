from fastapi import FastAPI, HTTPException, Depends, Request
from db import get_database
from typing import Any, Dict, List, Union, Annotated
from pydantic import BaseModel, Field
from bson import ObjectId
import uvicorn
from fastapi_clerk_auth import ClerkConfig, ClerkHTTPBearer, HTTPAuthorizationCredentials
import os
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware  # Import CORSMiddleware

load_dotenv()

JWT_SECRET = os.getenv("JWT_SECRET")
if JWT_SECRET is None:
    raise ValueError("JWT_SECRET environment variable is not set.")

clerk_config = ClerkConfig(jwks_url=JWT_SECRET)
clerk_auth_guard = ClerkHTTPBearer(config=clerk_config)

app = FastAPI()
mongo_client = get_database()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)


@app.get("/")
async def root(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(
        clerk_auth_guard)
):
    return {"message": "Hello World, you are signed in"}


class Report(BaseModel):
    userId: str
    sessionId: int | None = None
    courseId: int
    type: str
    device: str
    addedAt: int
    # accept a single body OR a list of bodies
    body: Union[Dict[str, Any], List[Dict[str, Any]]] = Field(
        ...,
        description="Single report payload or a list of payloads sent in one \
request",
    )


@app.post("/add/report")
async def add_report(
    report: Report,
    credentials: Annotated[
        HTTPAuthorizationCredentials, Depends(clerk_auth_guard)
    ],
):
    if mongo_client is None:
        raise HTTPException(
            status_code=503, detail="MongoDB connection failed")

    collection = mongo_client["test"]["reports"]
    # Convert pydantic model → dict
    report_dict = report.model_dump()

    # Split common metadata from body
    meta: Dict[str, Any] = {k: v for k,
                            v in report_dict.items() if k != "body"}
    body_payload = report_dict["body"]

    # ──────────────────────────────────────────────────────────
    # CASE 1: body is a LIST  →  insert_many
    # ──────────────────────────────────────────────────────────
    if isinstance(body_payload, list):
        docs: List[Dict[str, Any]] = [
            {**meta, "body": single_body} for single_body in body_payload
        ]

        try:
            result = collection.insert_many(docs)
        except Exception as exc:
            raise HTTPException(
                status_code=500, detail=f"Failed to insert reports: {exc}"
            )

        inserted_ids = [str(_id) for _id in result.inserted_ids]
        return {"status": "success", "insertedIds": inserted_ids}

    # ──────────────────────────────────────────────────────────
    # CASE 2: body is a single DICT  →  insert_one
    # ──────────────────────────────────────────────────────────
    try:
        result = collection.insert_one({**meta, "body": body_payload})
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Failed to insert report: {exc}"
        )

    return {"status": "success", "insertedId": str(result.inserted_id)}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=3100)
