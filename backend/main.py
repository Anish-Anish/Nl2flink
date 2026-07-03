"""
MegaStore Pulse API.

    uvicorn backend.main:app --port 8000 --reload

Every route lives under /api/v1 and returns plain JSON, so the Streamlit app in ui/
and any future React app talk to exactly the same backend.
"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from backend.config import get_settings
from backend.database import initialise_database
from backend.routers import auth_routes, business_routes, engineer_routes, monitor_routes
from backend.services.stack_clients import StackError

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)-7s | %(name)-38s | %(message)s")
logger = logging.getLogger(__name__)

settings = get_settings()


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Create the studio table if it is missing, and log what the backend talks to."""
    initialise_database()
    logger.info("MegaStore Pulse API ready — model server: %s, flink: %s",
                settings.model_server_url, settings.flink_sql_gateway_url)
    yield


app = FastAPI(title="MegaStore Pulse API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_routes.router)
app.include_router(business_routes.router)
app.include_router(engineer_routes.router)
app.include_router(monitor_routes.router)


@app.exception_handler(StackError)
def handle_stack_error(request: Request, error: StackError) -> JSONResponse:
    """A server in the stack failed — say which one, with a 502."""
    logger.error("%s failed on %s: %s", error.service, request.url.path, error.message)
    return JSONResponse(status_code=502,
                        content={"error": f"{error.service} is not responding",
                                 "detail": error.message})


@app.get("/health")
def health() -> dict:
    """Liveness probe for the API process itself."""
    return {"status": "ok"}
