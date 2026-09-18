"""FastAPI application factory and entry point."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import db, storage
from .errors import register_exception_handlers
from .routes import router


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI):
        storage.ensure_dirs()
        db.init_db()
        yield

    application = FastAPI(
        title="Axle Ultrasound Resumable Upload Service",
        version="1.0.0",
        lifespan=lifespan,
    )
    register_exception_handlers(application)
    application.include_router(router)
    return application


app = create_app()
