from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.marketplace.routers import auth, datasets, shares
from src.marketplace.services import CSRFMiddleware
from src.models.database import init_db
from src.utils.logging_setup import configure_logging

configure_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Delta Sharing Marketplace API", lifespan=lifespan)
app.add_middleware(CSRFMiddleware)
app.include_router(auth.router)
app.include_router(datasets.router)
app.include_router(shares.router)
