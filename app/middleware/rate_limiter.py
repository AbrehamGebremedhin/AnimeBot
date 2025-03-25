import time
from fastapi import Request, HTTPException, status
from ..db.redis_service import RedisService

class RateLimiter:
    """Rate limiting middleware using Redis"""
    
    def __init__(self, requests_per_minute: int = 60):
        self.redis_service = RedisService()
        self.requests_per_minute = requests_per_minute
        self.window_seconds = 60
    
    async def __call__(self, request: Request, call_next):
        # Extract client IP (or use X-Forwarded-For header in production)
        client_ip = request.client.host
        
        # Create a Redis key for this IP
        redis_key = f"rate_limit:{client_ip}"
        
        # Check if rate limit exceeded
        current_count = await self.redis_service.get_cache(redis_key, 0)
        
        if isinstance(current_count, bytes):
            current_count = int(current_count.decode('utf-8'))
        elif not isinstance(current_count, int):
            current_count = 0
            
        if current_count >= self.requests_per_minute:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded. Please try again later."
            )
        
        # Increment count
        await self.redis_service.set_cache(
            redis_key, 
            current_count + 1,
            expiry_seconds=self.window_seconds
        )
        
        # Process the request
        response = await call_next(request)
        
        # Add rate limit headers
        response.headers["X-RateLimit-Limit"] = str(self.requests_per_minute)
        response.headers["X-RateLimit-Remaining"] = str(
            max(0, self.requests_per_minute - (current_count + 1))
        )
        
        return response
