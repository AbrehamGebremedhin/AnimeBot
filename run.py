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

# Determine if running in development or production
def is_development():
    """Check if the application is running in development mode."""
    return os.environ.get("ENV", "development").lower() == "development"

def start_telegram_bot():
    """
    Start the Telegram bot.
    In development: runs in a subprocess
    In production: imports the module directly
    """
    try:
        logger.info("Starting Telegram bot...")
        
        # Get the path to the telegramBot.py file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        bot_path = os.path.join(script_dir, "telegramBot.py")
        
        if not os.path.exists(bot_path):
            logger.error(f"Telegram bot script not found at {bot_path}")
            return
        
        if is_development():
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
        else:
            # In production (like on Render), import and run directly
            logger.info("Starting Telegram bot in production mode (direct import)")
            try:
                # Import the telegramBot module dynamically
                import importlib.util
                spec = importlib.util.spec_from_file_location("telegramBot", bot_path)
                telegram_bot_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(telegram_bot_module)
                
                # Start the bot in a thread (if the module provides a start function)
                if hasattr(telegram_bot_module, "start_bot"):
                    threading.Thread(target=telegram_bot_module.start_bot, daemon=True).start()
                else:
                    logger.warning("telegramBot.py doesn't have a start_bot function. Please ensure it initializes correctly when imported.")
            except Exception as e:
                logger.error(f"Failed to import and start Telegram bot: {str(e)}")
                return
            
        logger.info("Telegram bot started successfully")
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
    
    # Start the API server
    uvicorn.run("run:app", host=host, port=port, reload=reload)
