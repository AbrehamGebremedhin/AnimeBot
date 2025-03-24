import uvicorn
import asyncio
import logging
from fastapi import FastAPI
from app.rest.routes import user_router, profile_router, chat_router, init_db, setup_app

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the FastAPI app
app = FastAPI(title="Anime Bot API")

# Register startup events
setup_app(app)

# Include routers
app.include_router(user_router)
app.include_router(profile_router)
app.include_router(chat_router)

# Explicitly initialize database before starting
@app.on_event("startup")
async def startup_db_client():
    try:
        await init_db()
        logger.info("Database initialized successfully on startup")
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        raise

if __name__ == "__main__":
    uvicorn.run("run:app", host="0.0.0.0", port=8000, reload=True)
