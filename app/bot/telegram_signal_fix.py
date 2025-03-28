import os
import logging
import signal
import threading
import asyncio

logger = logging.getLogger(__name__)

def safe_add_signal_handler(loop, sig, handler):
    """
    Safely add a signal handler that works in both main thread and other threads.
    In non-main threads, it will silently skip setting up signal handlers.
    
    Args:
        loop: The asyncio event loop
        sig: Signal number to handle
        handler: Callback function to call when signal is received
    """
    # Check if we're running in Docker or a non-main thread
    is_thread = threading.current_thread() is not threading.main_thread()
    is_signal_disabled = os.environ.get("TELEGRAM_NO_THREAD_SIGNALS") == "1"
    
    if is_thread or is_signal_disabled:
        logger.info(f"Skipping signal handler setup for signal {sig} in non-main thread")
        return False
    
    try:
        loop.add_signal_handler(sig, handler)
        return True
    except (NotImplementedError, RuntimeError) as e:
        logger.warning(f"Could not add signal handler: {e}")
        return False
