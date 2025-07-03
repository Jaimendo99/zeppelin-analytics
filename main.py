from fastapi import FastAPI, HTTPException, Depends, Request
from db import get_database
from typing import Any, Dict, List, Union, Annotated
from pydantic import BaseModel, Field
import uvicorn
from fastapi_clerk_auth import ClerkConfig, ClerkHTTPBearer, HTTPAuthorizationCredentials
import os
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
import dataframeloader as df_loader
from apiClient import APIClient
import asyncio
from contextlib import asynccontextmanager

# Load environment variables from .env file
load_dotenv()

# --------------------------------------------------------------------------
# Environment Variable and Configuration Setup
# --------------------------------------------------------------------------
JWT_SECRET = os.getenv("JWT_SECRET")
API_IDENTIFIER = os.getenv("API_IDENTIFIER")
API_PASSWORD = os.getenv("API_PASSWORD")

if not JWT_SECRET:
    raise ValueError("JWT_SECRET environment variable is not set.")
if not API_IDENTIFIER or not API_PASSWORD:
    raise ValueError("API_IDENTIFIER and API_PASSWORD must be set for the data lake loader.")

clerk_config = ClerkConfig(jwks_url=JWT_SECRET)
clerk_auth_guard = ClerkHTTPBearer(config=clerk_config)


# --------------------------------------------------------------------------
# Data Lake Loading and Refreshing Logic
# --------------------------------------------------------------------------
async def reload_lake_periodically(api_client: APIClient, db_client, interval_seconds: int):
    """Periodically reloads the data lake in the background."""
    while True:
        print("Starting periodic data lake refresh...")
        try:
            await df_loader.load_lake(api=api_client, db=db_client)
        except Exception as e:
            print(f"Error reloading data lake: {e}")
        await asyncio.sleep(interval_seconds)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handles application startup and shutdown events.
    """
    print("Application starting up...")
    # Initialize API and DB clients
    api_client = APIClient(identifier=API_IDENTIFIER, password=API_PASSWORD)
    db_client = get_database()

    # Perform initial data load
    print("Performing initial data lake load...")
    await df_loader.load_lake(api=api_client, db=db_client)

    # Schedule the background task to refresh the lake every 10 minutes (600 seconds)
    refresh_task = asyncio.create_task(
        reload_lake_periodically(api_client, db_client, 600)
    )

    yield  # Application is now running

    print("Application shutting down...")
    refresh_task.cancel()
    try:
        await refresh_task
    except asyncio.CancelledError:
        print("Data lake refresh task cancelled successfully.")


# --------------------------------------------------------------------------
# FastAPI Application Initialization
# --------------------------------------------------------------------------
app = FastAPI(lifespan=lifespan)
mongo_client = get_database()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --------------------------------------------------------------------------
# Pydantic Models for Request/Response
# --------------------------------------------------------------------------
class Report(BaseModel):
    userId: str
    sessionId: int | None = None
    courseId: int
    type: str
    device: str
    addedAt: int
    body: Union[Dict[str, Any], List[Dict[str, Any]]] = Field(
        ...,
        description="Single report payload or a list of payloads sent in one request",
    )


# --------------------------------------------------------------------------
# API Endpoints
# --------------------------------------------------------------------------
@app.get("/student/report/")
async def student_report(user_id: str, start_date: str, end_date: str,
     # request: Request,
     # credentials: HTTPAuthorizationCredentials | None = Depends(clerk_auth_guard)
 ):
    """
    Generates a student report from the in-memory data lake.
    """
    print("DATALAKE 🍎", df_loader.lake)
    if df_loader.lake.empty:
        raise HTTPException(
            status_code=503,
            detail="Data lake is not yet available. Please try again in a few moments."
        )
    # Assuming get_user_report is imported or defined elsewhere
    from service import get_user_report
    try:
        report = get_user_report(df_loader.lake, user_id, start_date, end_date)
    except ValueError as e:
        raise HTTPException(
            status_code=404,
            detail=str(e)
        )
    return report

@app.get("/teacher/report/")
async def teacher_report(
        teacher_id: str,
        request: Request,
        credentials: HTTPAuthorizationCredentials | None = Depends(clerk_auth_guard),
        start_date: str = None,
        end_date: str = None,
):
    """
    Generates a teacher report from the in-memory data lake.
    """
    if df_loader.lake.empty:
        raise HTTPException(
            status_code=503,
            detail="Data lake is not yet available. Please try again in a few moments."
        )
    # Assuming get_teacher_report is imported or defined elsewhere
    from metriccalc.teacherreport import get_teacher_report
    try:
        report = await get_teacher_report(
            api=APIClient(identifier=API_IDENTIFIER, password=API_PASSWORD),
            df=df_loader.lake,
            teacher_id=teacher_id,
            start_date=start_date,
            end_date=end_date
        )
    except ValueError as e:
        raise HTTPException(
            status_code=404,
            detail=str(e)
        )
    return report


@app.get("/")
async def root(
        request: Request,
        credentials: HTTPAuthorizationCredentials | None = Depends(clerk_auth_guard)
):
    """
    A protected root endpoint to verify authentication.
    """
    return {"message": "Hello World, you are signed in"}


@app.post("/add/report")
async def add_report(
        report: Report,
        credentials: Annotated[
            HTTPAuthorizationCredentials, Depends(clerk_auth_guard)
        ],
):
    """
    Adds a new report or a batch of reports to the database.
    """
    if mongo_client is None:
        raise HTTPException(status_code=503, detail="MongoDB connection failed")

    collection = mongo_client["test"]["reports"]
    report_dict = report.model_dump()
    meta: Dict[str, Any] = {k: v for k, v in report_dict.items() if k != "body"}
    body_payload = report_dict["body"]

    if isinstance(body_payload, list):
        docs: List[Dict[str, Any]] = [
            {**meta, "body": single_body} for single_body in body_payload
        ]
        try:
            result = collection.insert_many(docs)
            inserted_ids = [str(_id) for _id in result.inserted_ids]
            return {"status": "success", "insertedIds": inserted_ids}
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Failed to insert reports: {exc}")
    else:
        try:
            result = collection.insert_one({**meta, "body": body_payload})
            return {"status": "success", "insertedId": str(result.inserted_id)}
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Failed to insert report: {exc}")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=3100)
