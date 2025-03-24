import uvicorn
import asyncio
import logging
from fastapi import FastAPI
from app.rest.routes import user_router, profile_router, chat_router
from app.db.database import init_db

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the FastAPI app
app = FastAPI(title="Anime Bot API")

# Register shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources when shutting down"""
    logger.info("Shutting down application, cleaning up resources...")
    
    # Allow time for pending tasks to complete
    pending = asyncio.all_tasks()
    logger.info(f"Waiting for {len(pending)} pending tasks to complete...")
    
    # Give tasks some time to complete
    for task in pending:
        try:
            # Only wait for a short time to avoid hanging shutdown
            await asyncio.wait_for(task, timeout=2.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass
        except Exception as e:
            logger.error(f"Error in task during shutdown: {str(e)}")
    
    logger.info("Application shutdown complete")

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
