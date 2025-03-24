from fastapi import APIRouter, HTTPException, Depends, status, FastAPI
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import asyncio
import concurrent.futures
from functools import partial
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
import logging

from ..db.models import User
from ..db.database import get_db  # Assuming you have a database connection utility
# Fix imports to use relative paths
from ..chat_processor.chatbot import Chat
from ..chat_processor.user_management import UserService
from ..utils.mal_api import API_CALL
from ..db.sqlite_service import SQLiteService  # Import SQLiteService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Router instances
user_router = APIRouter(prefix="/users", tags=["users"])
profile_router = APIRouter(prefix="/profile", tags=["profile"])
chat_router = APIRouter(prefix="/chat", tags=["chat"])

# Thread pool for CPU-bound tasks
thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)

# Pydantic models
class UserCreate(BaseModel):
    username: str
    
    class Config:
        orm_mode = True

class UserResponse(BaseModel):
    id: int
    username: str
    
    class Config:
        orm_mode = True

class ProfileRequest(BaseModel):
    user_id: int
    category: str

class ProfileUpdateRequest(BaseModel):
    user_id: int
    category: str
    fields: Dict[str, Any]

class ChatRequest(BaseModel):
    user_id: int
    reply: Optional[str] = ""

# Database initialization
async def init_db():
    """Initialize the database and create tables on application startup"""
    try:
        sqlite_service = SQLiteService()
        await sqlite_service.create_tables()
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Error creating database tables: {str(e)}")
        raise

# Initialize database immediately
# This ensures tables are created regardless of whether FastAPI startup events run
asyncio.run(init_db())
logger.info("Database initialization complete")

# Function to register startup event with FastAPI
def setup_app(app: FastAPI):
    """Register startup event with FastAPI application"""
    app.add_event_handler("startup", init_db)
    logger.info("Registered database initialization event")

# User routes - only keeping list and create
@user_router.get("/", response_model=List[UserResponse])
async def list_users(db: AsyncSession = Depends(get_db)):
    """List all users"""
    try:
        result = await db.execute(select(User))
        users = result.scalars().all()
        return users
    except Exception as e:
        logger.error(f"Error listing users: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}"
        )

@user_router.post("/", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def create_user(user: UserCreate, db: AsyncSession = Depends(get_db)):
    """Create a new user"""
    try:
        # Create user in SQL database
        db_user = User(username=user.username)
        db.add(db_user)
        await db.commit()
        await db.refresh(db_user)
        
        # Create user in Neo4j graph database
        user_service = UserService()
        await user_service.create_user(user_id=db_user.id, username=db_user.username)
        
        return db_user
    except Exception as e:
        await db.rollback()
        logger.error(f"Error creating user: {str(e)}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

# Profile routes
@profile_router.get("/questions")
async def get_profile_questions(user_id: int, category: str):
    """Get profile questions by category"""
    questions = await get_chat_questions(
        user_id=user_id,
        category=category
    )
    return questions

@profile_router.post("/update", status_code=status.HTTP_202_ACCEPTED)
async def update_profile(update_request: ProfileUpdateRequest):
    """Update user profile fields"""
    service = UserService()
    await update_user_profile(
        service,
        update_request.user_id,
        update_request.category,
        update_request.fields
    )
    return {"status": "accepted"}

# Chat routes
@chat_router.post("/")
async def chat_with_bot(chat_request: ChatRequest):
    """Chat with the bot"""
    try:
        user_id = chat_request.user_id
        user_reply = chat_request.reply
        
        # Initialize chat instance
        chat = Chat(user_id=user_id)
        mal_api = API_CALL()
        
        if user_reply == "/recommend":
            # Handle recommendation request
            response = await get_chat_response(chat, user_id, None, user_reply)
            recommendations = await process_recommendations(response["Recommendations"], mal_api)
            await chat.reset_session_history()
            return recommendations
        
        elif user_reply == "":
            # Generate response for empty user input
            response = await get_chat_response(chat, user_id, None, "Hello")
            await chat.save_session_history()
            return response
        
        else:
            # Generate response for normal user input
            response = await get_chat_response(chat, user_id, None, user_reply)
            await chat.save_session_history()
            return response
            
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

# Helper functions (these would need to be implemented with your actual database logic)
async def get_all_users():
    """Get all users asynchronously"""
    # This should be implemented with your ORM's async methods
    pass

async def create_new_user(data):
    """Create a new user asynchronously"""
    # This should be implemented with your ORM's async methods
    pass

# Other helper functions for profile and chat routes
async def get_chat_questions(user_id: str, category: str):
    """
    Get chat questions for a specific category.
    
    Args:
        user_id (str): The user ID.
        category (str): The question category.
        
    Returns:
        dict: The questions for the specified category.
    """
    chat = await Chat(user_id).initialize()
    questions = await chat.chat(user_id, "", category)  # Add await here
    return questions[category]  # Now questions is a dict, not a coroutine

async def update_user_profile(service, user_id, category, fields):
    """Update user profile asynchronously"""
    tasks = []
    for key, value in fields.items():
        task = asyncio.to_thread(
            service.update_variable, 
            user_id=user_id, 
            field_name=f"{category}_{key}", 
            value=value
        )
        tasks.append(task)
    await asyncio.gather(*tasks)

async def get_chat_response(chat, user_id, category, user_req):
    """Get chat response asynchronously"""
    # Since chat.chat is now properly async, we don't need to_thread here
    return await chat.chat(req_user=user_id, category=category, user_req=user_req)

async def process_recommendations(recommendations, mal_api):
    """Process anime recommendations concurrently"""
    tasks = []
    for recommendation in recommendations:
        task = get_anime_data(mal_api, recommendation)
        tasks.append(task)
    return await asyncio.gather(*tasks)

async def get_anime_data(mal_api, recommendation):
    """Get anime data asynchronously"""
    url, synopsis = await asyncio.to_thread(
        mal_api.anime_data, recommendation["title"]
    )
    recommendation["image_url"] = url
    recommendation["synopsis"] = synopsis
    return recommendation

# Register routers in your main app
# In your main.py or similar:
# app = FastAPI()
# setup_app(app)  # Register the startup event
# app.include_router(user_router)
# app.include_router(profile_router)
# app.include_router(chat_router)
