from fastapi import APIRouter, HTTPException, Depends, status, FastAPI, BackgroundTasks
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
from app.utils.diversity_service import DiversityService

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
        
        # Use API_CALL as a context manager to ensure proper cleanup
        async with API_CALL() as mal_api:
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
        logger.error(f"Error in chat_with_bot: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while processing the chat request: {str(e)}"
        )

@chat_router.post("/fresh-recommendations")
async def get_fresh_recommendations(
    user_data: dict,
    background_tasks: BackgroundTasks
):
    """
    Get fresh anime recommendations, ensuring no repeats from previous recommendations
    """
    try:
        user_id = user_data.get("user_id", "")
        if not user_id:
            return {"error": "User ID is required"}
            
        # Clear previous recommendations for this user
        neo4j_connection = Neo4jConnection()
        diversity_service = DiversityService(neo4j_connection)
        
        # Clear in background to not delay response
        background_tasks.add_task(clear_user_recent_recommendations, user_id)
        
        # Get recommendations through regular endpoint
        chat_instance = await Chat(user_id).initialize()
        user_profile = await chat_instance.user_service.get_user_profile(user_id)
        
        # Get recommendations
        recommendations = await chat_instance.similarity_search(user_profile)
        
        return recommendations
        
    except Exception as e:
        logger.error(f"Error getting fresh recommendations: {str(e)}")
        return {"error": f"Failed to get recommendations: {str(e)}"}

async def clear_user_recent_recommendations(user_id: str, days: int = 1):
    """
    Clear only recent recommendations (last day) to allow for fresh recommendations
    while preserving longer-term history
    """
    try:
        neo4j_connection = Neo4jConnection()
        
        current_time = int(time.time() * 1000)  # Current time in milliseconds
        time_threshold = current_time - (days * 24 * 60 * 60 * 1000)  # Last day
        
        async with neo4j_connection.get_async_driver().session() as session:
            query = """
            MATCH (u:User {user_id: $user_id})-[r:RECEIVED_RECOMMENDATION]->(a:Anime)
            WHERE r.timestamp > $time_threshold
            DELETE r
            RETURN count(r) as deleted_count
            """
            result = await session.run(query, {
                "user_id": user_id,
                "time_threshold": time_threshold
            })
            record = await result.single()
            deleted_count = record["deleted_count"] if record else 0
            
            logger.info(f"Cleared {deleted_count} recent recommendations for user {user_id}")
            
    except Exception as e:
        logger.error(f"Error clearing recent recommendations: {str(e)}")

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
        # Directly call the async method instead of using asyncio.to_thread
        task = service.update_variable(
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
    try:
        if not recommendations or not isinstance(recommendations, list):
            logger.error(f"Invalid recommendations format: {recommendations}")
            return []
        
        tasks = []
        for recommendation in recommendations:
            task = get_anime_data(mal_api, recommendation)
            tasks.append(task)
        
        return await asyncio.gather(*tasks)
    except Exception as e:
        logger.error(f"Error processing recommendations: {str(e)}")
        return recommendations  # Return original recommendations if processing fails

async def get_anime_data(mal_api, recommendation):
    """Get anime data asynchronously"""
    try:
        # Ensure recommendation has required fields
        if not recommendation or not isinstance(recommendation, dict):
            logger.error(f"Invalid recommendation format: {recommendation}")
            return {}
            
        if "title" not in recommendation:
            logger.error(f"Missing title in recommendation: {recommendation}")
            return recommendation
        
        # Check if anime_data is an async method
        if asyncio.iscoroutinefunction(mal_api.anime_data):
            # If it's async, await it directly
            url, synopsis = await mal_api.anime_data(recommendation["title"])
        else:
            # If it's synchronous, use to_thread
            try:
                url, synopsis = await asyncio.to_thread(
                    mal_api.anime_data, recommendation["title"]
                )
            except Exception as e:
                logger.error(f"Error in asyncio.to_thread for anime_data: {str(e)}")
                # Don't await if it's a coroutine object created by to_thread
                if asyncio.iscoroutine(mal_api.anime_data(recommendation["title"])):
                    logger.info("anime_data is a coroutine, calling directly")
                    url, synopsis = await mal_api.anime_data(recommendation["title"])
                else:
                    # Fallback to direct call
                    url, synopsis = mal_api.anime_data(recommendation["title"])
        
        # Only update if we got valid data
        if url:
            recommendation["image_url"] = url
        if synopsis:
            recommendation["synopsis"] = synopsis
        
        return recommendation
    except Exception as e:
        logger.error(f"Error getting anime data for {recommendation.get('title', 'unknown')}: {str(e)}")
        return recommendation

# Register routers in your main app
# In your main.py or similar:
# app = FastAPI()
# setup_app(app)  # Register the startup event
# app.include_router(user_router)
# app.include_router(profile_router)
# app.include_router(chat_router)
