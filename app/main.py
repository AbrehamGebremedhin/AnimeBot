from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import logging
from .db.database import init_db
from .rest.routes import user_router, profile_router, chat_router
from .rest.health import health_router
from .rest.tasks import tasks_router
from .utils.neo4j_connection import Neo4jConnection
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
    
    # Close Neo4j connections
    neo4j_connection = Neo4jConnection()
    neo4j_connection.close()
    
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
