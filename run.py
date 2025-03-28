import uvicorn
import asyncio
import logging
import subprocess
import threading
import os
import time
import sys
from fastapi import FastAPI
from app.rest.routes import user_router, profile_router, chat_router
from app.db.database import init_db
# Add import for RedisService
from app.db.redis_service import RedisService
# Add import for Neo4jConnection
from app.utils.neo4j_connection import Neo4jConnection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the FastAPI app
app = FastAPI(title="Anime Bot API")

# Add a redirect from root to docs
from fastapi.responses import RedirectResponse

# Import health router if it exists, or create a simple one
try:
    from app.rest.health import health_router
    logger.info("Imported health_router from app.rest.health")
except ImportError:
    # Create a basic health router
    from fastapi import APIRouter
    health_router = APIRouter(prefix="/health", tags=["health"])
    
    @health_router.get("/liveness")
    async def liveness():
        return {"status": "alive"}
    
    @health_router.get("/readiness")
    async def readiness():
        return {"status": "ready"}
    
    logger.info("Created basic health endpoints")

@app.get("/")
async def root():
    """Redirect root to docs for better user experience"""
    return RedirectResponse(url="/docs")

# Global Redis service instance
redis_service = None
# Global Telegram bot thread instance
telegram_bot_thread = None

# Register startup event
@app.on_event("startup")
async def startup_event():
    global redis_service, telegram_bot_thread
    # Initialize Redis service
    redis_service = RedisService()
    try:
        await init_db()
        logger.info("Database initialized successfully on startup")
        
        # Start the Telegram bot in production mode
        if not is_development():
            # In production, import and start the bot directly
            try:
                from app.bot import telegramBot
                # Start the bot and keep track of the thread
                telegram_bot_thread = telegramBot.start_bot()
                logger.info("Telegram bot started successfully in production mode")
            except Exception as e:
                logger.error(f"Failed to start Telegram bot: {e}")
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        raise

# Register shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources when shutting down"""
    logger.info("Shutting down application, cleaning up resources...")
    
    # First, shutdown the Telegram bot if it's running
    global telegram_bot_thread
    if telegram_bot_thread:
        try:
            # Import the shutdown function
            from app.bot.telegramBot import shutdown_bot
            # Shutdown the Telegram bot
            shutdown_bot(telegram_bot_thread)
        except Exception as e:
            logger.error(f"Error shutting down Telegram bot: {str(e)}")
    
    # Clear thread reference
    telegram_bot_thread = None
    
    # Close database connections
    try:
        # Close Redis connections first (important for clean shutdown)
        global redis_service
        if redis_service:
            await redis_service.close()
            logger.info("Redis connection closed")
            # Clear the reference immediately
            redis_service = None
        
        # Then close Neo4j connections
        neo4j_connection = Neo4jConnection()
        # Close sync driver
        neo4j_connection.close()
        # Close async driver properly
        await neo4j_connection.close_async()
        logger.info("Neo4j connection closed")
        
        # Clear Neo4j singleton instance to force recreation later if needed
        Neo4jConnection._instance = None
    except Exception as e:
        logger.error(f"Error closing database connections: {str(e)}")
    
    # Now handle pending tasks more safely - with a longer timeout
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    
    if tasks:
        logger.info(f"Waiting for {len(tasks)} pending tasks to complete...")
        
        # Give tasks more time to complete
        try:
            # Wait with a timeout - increased to 5 seconds
            done, pending = await asyncio.wait(tasks, timeout=5.0)
            
            # Cancel any remaining tasks
            if pending:
                logger.info(f"Cancelling {len(pending)} remaining tasks")
                for task in pending:
                    task.cancel()
                
                # Wait longer to let cancellation complete
                try:
                    await asyncio.wait(pending, timeout=2.0)
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"Error during task cancellation: {str(e)}")
        except Exception as e:
            logger.error(f"Error waiting for tasks during shutdown: {str(e)}")
    
    # Suggest garbage collection to clean up resources
    try:
        import gc
        gc.collect()
    except Exception as e:
        logger.error(f"Error during garbage collection: {str(e)}")
    
    logger.info("Application shutdown complete")

# Include routers
app.include_router(user_router)
app.include_router(profile_router)
app.include_router(chat_router)
app.include_router(health_router)  # Add health router

# Determine if running in development or production
def is_development():
    """Check if the application is running in development mode."""
    return os.environ.get("ENV", "development").lower() == "development"

def start_telegram_bot():
    """
    Start the Telegram bot.
    In development: runs in a subprocess
    In production: imports the module directly (this is now handled in startup_event)
    """
    # Only run in development mode, as production now starts the bot in startup_event
    if not is_development():
        logger.info("Skipping development mode Telegram bot start since we're in production")
        return
        
    try:
        logger.info("Starting Telegram bot in development mode...")
        
        # Make sure the correct backend API URL is set
        backend_api_url = os.environ.get("BACKEND_API_URL") 
        logger.info(f"Using BACKEND_API_URL: {backend_api_url}")
        
        # Get the path to the telegramBot.py file in the app/bot module
        script_dir = os.path.dirname(os.path.abspath(__file__))
        bot_path = os.path.join(script_dir, "app", "bot", "telegramBot.py")
        
        if not os.path.exists(bot_path):
            logger.error(f"Telegram bot script not found at {bot_path}")
            return
        
        # In development, run as subprocess for easier debugging
        logger.info("Starting Telegram bot in development mode (subprocess)")
        bot_process = subprocess.Popen(
            [sys.executable, "-u", bot_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )
        
        # Log stdout and stderr
        def log_output(stream, log_func):
            for line in stream:
                log_func(f"[TelegramBot] {line.strip()}")
        
        # Start threads to log output
        threading.Thread(target=log_output, args=(bot_process.stdout, logger.info), daemon=True).start()
        threading.Thread(target=log_output, args=(bot_process.stderr, logger.error), daemon=True).start()
        
        # Monitor the process
        def monitor_process():
            return_code = bot_process.wait()
            if return_code != 0:
                logger.error(f"Telegram bot process exited with code {return_code}")
        
        threading.Thread(target=monitor_process, daemon=True).start()
        logger.info("Telegram bot started successfully in development mode")
    except Exception as e:
        logger.error(f"Failed to start Telegram bot: {str(e)}")

if __name__ == "__main__":
    # Start the Telegram bot
    threading.Thread(target=start_telegram_bot, daemon=True).start()
    
    # Get port and host from environment variables (for Render compatibility)
    port = int(os.environ.get("PORT", 8050))
    host = "0.0.0.0"  # Bind to all interfaces
    
    # Disable reload in production environments
    reload = is_development()
    
    # Log the server configuration
    env = "development" if reload else "production"
    logger.info(f"Starting server in {env} mode on {host}:{port} (reload: {reload})")
    
    # Start the API server with proper exception handling
    try:
        # Use uvicorn's run directly, which handles signal management
        uvicorn.run("run:app", host=host, port=port, reload=reload)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Received shutdown signal. Exiting gracefully...")
    except Exception as e:
        logger.error(f"Server error: {str(e)}")
    finally:
        # Skip task cleanup as uvicorn handles this
        # The previous errors occurred during uvicorn's own cleanup process
        # which we shouldn't try to interfere with
        logger.info("Application shut down successfully")
