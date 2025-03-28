from typing import List, Optional
import os
import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.future import select

from .models import Base, User

logger = logging.getLogger(__name__)

class SQLiteService:
    def __init__(self, database_url: str = None):
        # Use a path in the data directory that will be mounted as a volume
        if database_url is None:
            # Use the data directory that is mounted as a volume
            data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data')
            os.makedirs(data_dir, exist_ok=True)
            database_path = os.path.join(data_dir, 'app.db')
            database_url = f"sqlite+aiosqlite:///{database_path}"
        
        logger.info(f"Using SQLite database URL: {database_url}")
        
        self.engine = create_async_engine(
            database_url, 
            echo=False,
            connect_args={"check_same_thread": False}
        )
        self.async_session = sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        
    async def create_tables(self):
        """Create all tables defined in models"""
        try:
            logger.info("Creating database tables...")
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            logger.info("Database tables created successfully")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    # User-related operations
    async def create_user(self, username: str) -> User:
        """Create a new user with the given username"""
        async with self.async_session() as session:
            new_user = User(username=username, id=username)
            session.add(new_user)
            await session.commit()
            await session.refresh(new_user)
            return new_user
    
    async def get_user_by_id(self, user_id: str) -> Optional[User]:
        """Get a user by their ID"""
        async with self.async_session() as session:
            result = await session.execute(select(User).where(User.id == user_id))
            return result.scalars().first()
    
    async def get_user_by_username(self, username: str) -> Optional[User]:
        """Get a user by their username"""
        async with self.async_session() as session:
            result = await session.execute(select(User).where(User.username == username))
            return result.scalars().first()
    
    async def get_all_users(self) -> List[User]:
        """Get all users"""
        async with self.async_session() as session:
            result = await session.execute(select(User))
            return result.scalars().all()
    
    async def update_username(self, user_id: str, new_username: str) -> Optional[User]:
        """Update a user's username"""
        async with self.async_session() as session:
            user = await session.get(User, user_id)
            if user:
                user.username = new_username
                await session.commit()
                return user
            return None
    
    async def delete_user(self, user_id: str) -> bool:
        """Delete a user by their ID"""
        async with self.async_session() as session:
            user = await session.get(User, user_id)
            if user:
                await session.delete(user)
                await session.commit()
                return True
            return False
