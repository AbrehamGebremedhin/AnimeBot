from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text  # Add this import
import os
import logging
from .models import Base

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure SQLite database with absolute path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(BASE_DIR, "app.db")
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite+aiosqlite:///{DB_PATH}")

logger.info(f"Database URL: {DATABASE_URL}")
logger.info(f"Database path: {DB_PATH}")

# Create async engine
engine = create_async_engine(
    DATABASE_URL, 
    echo=True,
    future=True,
    # Add connect_args for better SQLite async support
    connect_args={"check_same_thread": False}
)

# Create session
async_session = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

async def get_db():
    """Dependency for database session"""
    async with async_session() as session:
        try:
            yield session
        finally:
            await session.close()

async def handle_neo4j_warnings(warning_message):
    """
    Handle Neo4j specific warnings to prevent log clutter.
    
    Args:
        warning_message: The warning message from Neo4j
    """
    # Check if it's a common aggregation null value warning
    if "null value eliminated in set function" in str(warning_message):
        # Log at debug level instead of warning level
        logger.debug(f"Neo4j aggregation warning (expected behavior): {warning_message}")
    else:
        # Log other Neo4j warnings normally
        logger.warning(f"Neo4j warning: {warning_message}")

async def init_db():
    """Initialize the database by creating all tables"""
    try:
        # Check if database file exists
        db_file_exists = os.path.exists(DB_PATH)
        if db_file_exists:
            logger.info(f"Database file already exists at {DB_PATH}")
        else:
            logger.info(f"Database file doesn't exist, will be created at {DB_PATH}")
        
        # Drop all tables and recreate them to ensure a clean state
        async with engine.begin() as conn:
            # Drop all tables first
            # await conn.run_sync(Base.metadata.drop_all)
            # Then create all tables
            await conn.run_sync(Base.metadata.create_all)
        
        logger.info(f"Database tables created successfully at {DB_PATH}")
        
        # Verify the tables were created
        async with engine.connect() as conn:
            # Run the query to get table names
            result = await conn.run_sync(lambda sync_conn: sync_conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table';")
            ))
            # Get all rows from the result object - no need to await fetchall()
            rows = result.fetchall()
            table_names = [table[0] for table in rows]
            logger.info(f"Tables in database: {table_names}")
            
            # Specifically check for users table
            if 'users' in table_names:
                logger.info("Users table exists!")
            else:
                logger.error("Users table was not created!")
                
            # Log table structure
            for table_name in table_names:
                if table_name.startswith('sqlite'):
                    continue
                # Run query to get table info
                table_info_result = await conn.run_sync(lambda sync_conn: sync_conn.execute(
                    text(f"PRAGMA table_info({table_name});")
                ))
                # Get columns directly from result - no need to await fetchall()
                columns = table_info_result.fetchall()
                logger.info(f"Table {table_name} structure: {columns}")
            
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise
