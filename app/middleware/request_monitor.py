import time
import logging
from fastapi import Request

logger = logging.getLogger(__name__)

class RequestMonitorMiddleware:
    """Middleware for monitoring request performance"""
    
    async def __call__(self, request: Request, call_next):
        # Record start time
        start_time = time.time()
        
        # Process request
        try:
            response = await call_next(request)
            
            # Calculate and log processing time
            process_time = time.time() - start_time
            response.headers["X-Process-Time"] = str(process_time)
            
            # Log request details for monitoring
            logger.info(
                f"Request: {request.method} {request.url.path} "
                f"Status: {response.status_code} "
                f"Time: {process_time:.4f}s"
            )
            
            return response
        except Exception as e:
            # Log exceptions
            process_time = time.time() - start_time
            logger.error(
                f"Request: {request.method} {request.url.path} "
                f"Error: {str(e)} "
                f"Time: {process_time:.4f}s"
            )
            raise
