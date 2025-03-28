from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import logging
from .db.database import init_db
from .rest.routes import user_router, profile_router, chat_router
from .rest.health import health_router
from .rest.tasks import tasks_router
from .utils.neo4j_connection import Neo4jConnection
from .db.redis_service import RedisService
from .middleware.rate_limiter import RateLimiter
from .middleware.request_monitor import RequestMonitorMiddleware
from app.rest.maintenance import maintenance_router

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create the FastAPI app
app = FastAPI(
    title="AnimeBot API",
    description="Backend API for the AnimeBot application",
    version="0.1.0",
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add request monitoring middleware
app.add_middleware(RequestMonitorMiddleware)

# Add rate limiting middleware
app.add_middleware(RateLimiter, requests_per_minute=120)  # Adjust as needed

# Variable to track if database has been initialized
db_initialized = False

@app.on_event("startup")
async def startup_event():
    """Initialize database tables on application startup"""
    global db_initialized
    
    logger.info("Starting database initialization...")
    try:
        # Initialize the database
        await init_db()
        db_initialized = True
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        # In a production environment, you might want to exit the application here
        import sys
        logger.error("Exiting application due to database initialization failure")
        sys.exit(1)

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources when shutting down"""
    logger.info("Shutting down application, cleaning up resources...")
    
    # First close Redis connections
    try:
        redis_service = RedisService()
        await redis_service.close()
        logger.info("Redis services closed")
    except Exception as e:
        logger.error(f"Error closing Redis connection: {str(e)}")
    
    # Then close Neo4j connections
    try:
        neo4j_connection = Neo4jConnection()
        neo4j_connection.close()
        logger.info("Neo4j connection closed")
    except Exception as e:
        logger.error(f"Error closing Neo4j connection: {str(e)}")
    
    # Allow time for pending tasks to complete
    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    logger.info(f"Waiting for {len(pending)} pending tasks to complete...")
    
    # Give tasks some time to complete
    if pending:
        try:
            # Wait for a short time with timeout to avoid hanging shutdown
            await asyncio.wait(pending, timeout=3.0)
            
            # Cancel any remaining tasks
            remaining = [t for t in pending if not t.done()]
            if remaining:
                logger.info(f"Cancelling {len(remaining)} remaining tasks")
                for task in remaining:
                    task.cancel()
                
                # Wait briefly to let cancellation complete
                await asyncio.wait(remaining, timeout=1.0)
        except Exception as e:
            logger.error(f"Error waiting for tasks during shutdown: {str(e)}")
    
    logger.info("Application shutdown complete")

# Include routers only after database initialization
app.include_router(user_router, tags=["users"])
app.include_router(profile_router, tags=["profile"])
app.include_router(chat_router, tags=["chat"])
app.include_router(health_router, tags=["health"])
app.include_router(tasks_router, tags=["tasks"])
app.include_router(maintenance_router)

@app.get("/")
async def root():
    return {
        "message": "AnimeBot API is running", 
        "db_initialized": db_initialized
    }
