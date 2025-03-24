from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import logging
from .db.database import init_db
from .rest.routes import user_router, profile_router, chat_router

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

# Include routers only after database initialization
app.include_router(user_router, tags=["users"])
app.include_router(profile_router, tags=["profile"])
app.include_router(chat_router, tags=["chat"])

@app.get("/")
async def root():
    return {
        "message": "AnimeBot API is running", 
        "db_initialized": db_initialized
    }
