from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.delta_sharing.routers import delivery, dev, protocol, seller
from src.seller.database import init_seller_db
from src.utils.logging_setup import configure_logging

configure_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_seller_db()
    yield


app = FastAPI(title="Delta Sharing Server", lifespan=lifespan)
app.include_router(protocol.router)
app.include_router(seller.router)
app.include_router(delivery.router)
app.include_router(dev.router)
