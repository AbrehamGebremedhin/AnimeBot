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

def install_required_packages():
    """Check and install required packages for the Telegram bot."""
    required_packages = ['python-telegram-bot', 'python-dotenv', 'requests']
    
    try:
        logger.info("Checking and installing required packages...")
        # Check if pip is available
        subprocess.check_call([sys.executable, "-m", "pip", "--version"], 
                             stdout=subprocess.DEVNULL, 
                             stderr=subprocess.DEVNULL)
        
        # Install each required package if not already installed
        for package in required_packages:
            try:
                # Try importing to check if package is installed
                if package == 'python-telegram-bot':
                    module_name = 'telegram'
                elif package == 'python-dotenv':
                    module_name = 'dotenv'
                else:
                    module_name = package
                
                __import__(module_name)
                logger.info(f"Package {package} is already installed")
            except ImportError:
                logger.info(f"Installing missing package: {package}")
                subprocess.check_call([
                    sys.executable, "-m", "pip", "install", package
                ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                logger.info(f"Successfully installed {package}")
        
        return True
    except Exception as e:
        logger.error(f"Failed to install required packages: {str(e)}")
        return False

def start_telegram_bot():
    """
    Start the Telegram bot in a separate process.
    This function is called in a separate thread.
    """
    # Wait for API server to be fully operational
    time.sleep(5)
    
    try:
        logger.info("Starting Telegram bot...")
        
        # Install required packages
        if not install_required_packages():
            logger.error("Cannot start Telegram bot due to missing dependencies")
            logger.error("Please manually install required packages: python-telegram-bot python-dotenv requests")
            return
        
        # Get path to telegramBot.py
        script_dir = os.path.dirname(os.path.abspath(__file__))
        bot_path = os.path.join(script_dir, "telegramBot.py")
        
        if not os.path.exists(bot_path):
            logger.error(f"Telegram bot script not found at {bot_path}")
            return
        
        # Start the bot as a subprocess
        # Using python -u to ensure unbuffered output
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
        
        # Monitor the process to report if it exits unexpectedly
        def monitor_process():
            return_code = bot_process.wait()
            if return_code != 0:
                logger.error(f"Telegram bot process exited with code {return_code}")
        
        threading.Thread(target=monitor_process, daemon=True).start()
        
        logger.info("Telegram bot started successfully")
    except Exception as e:
        logger.error(f"Failed to start Telegram bot: {str(e)}")

if __name__ == "__main__":
    # Start the Telegram bot in a separate thread after the API server starts
    threading.Thread(target=start_telegram_bot, daemon=True).start()
    
    # Start the API server
    uvicorn.run("run:app", host="0.0.0.0", port=8000, reload=True)
