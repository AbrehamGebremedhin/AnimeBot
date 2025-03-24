from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes import user_router, profile_router, chat_router

# Create FastAPI application
app = FastAPI(
    title="AnimeBot API",
    description="API for anime recommendation and chat services",
    version="1.0.0"
)

# Configure CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For production, specify actual origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers from routes.py
app.include_router(user_router)
app.include_router(profile_router)
app.include_router(chat_router)

# Health check endpoint
@app.get("/health", tags=["health"])
async def health_check():
    """Health check endpoint to verify API is running"""
    return {
        "status": "healthy",
        "service": "animebot-backend"
    }

# Root endpoint
@app.get("/", tags=["root"])
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Welcome to AnimeBot API",
        "docs": "/docs"
    }

