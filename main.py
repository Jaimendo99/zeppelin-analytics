from typing import Annotated
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.security import HTTPAuthorizationCredentials
from db import get_database
from pydantic import BaseModel
from bson import ObjectId
import uvicorn
from fastapi_clerk_auth import ClerkConfig, ClerkHTTPBearer, HTTPAuthorizationCredentials
import os
from dotenv import load_dotenv

load_dotenv()

JWT_SECRET = os.getenv("JWT_SECRET")
if JWT_SECRET is None:
    raise ValueError("JWT_SECRET environment variable is not set.")

clerk_config = ClerkConfig(jwks_url=JWT_SECRET)
clerk_auth_guard = ClerkHTTPBearer(config=clerk_config)

app = FastAPI()


@app.get("/")
async def root(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(
        clerk_auth_guard)
):
    return {"message": "Hello World, you are signed in"}


mongo_client = get_database()


class Report(BaseModel):
    userId: str
    sessionId: str | None
    type: str
    device: str
    addedAt: int
    body: dict


@app.post("/add/report")
async def add_report(report: Report,
                     credentials: Annotated[HTTPAuthorizationCredentials, Depends(
                         clerk_auth_guard)]
                     ):
    if mongo_client is None:
        raise HTTPException(
            status_code=503, detail="MongoDB connection failed")

    db = mongo_client["test"]
    collection = db["reports"]
    report_dict = report.model_dump() if hasattr(
        report, "model_dump") else report.dict()

    try:
        result = collection.insert_one(report_dict)
        if not result.acknowledged or not result.inserted_id:
            raise HTTPException(status_code=500,
                                detail="Failed to insert report: No ID returned or operation not acknowledged.")
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to insert report: {e}")

    if result.inserted_id:
        report_dict["_id"] = str(result.inserted_id)
    elif "_id" in report_dict and isinstance(report_dict["_id"], ObjectId):
        report_dict["_id"] = str(report_dict["_id"])
    else:
        raise HTTPException(
            status_code=500, detail="Report inserted but failed to retrieve its ID for the response.")

    return {"status": "success", "report": report_dict}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=3100)
