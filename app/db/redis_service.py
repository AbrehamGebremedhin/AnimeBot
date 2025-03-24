import os
import json
import redis.asyncio as redis
from dotenv import load_dotenv
from typing import Any, Dict, Optional, Union

class RedisService:
    """Service to handle Redis caching operations asynchronously."""
    
    def __init__(self):
        """Initialize Redis connection using environment variables."""
        load_dotenv()
        
        # Parse Redis endpoint
        redis_endpoint = os.getenv("REDIS_ENDPOINT", "")
        host, port = redis_endpoint.split(":") if ":" in redis_endpoint else (redis_endpoint, 18369)
        
        # Connect to Redis using async client
        self.redis_client = redis.Redis(
            host=host,
            port=int(port),
            password=os.getenv("REDIS_PASSWORD", ""),
            decode_responses=False,  # Keep binary data as is
            socket_timeout=5,
        )
    
    async def set_cache(self, key: str, value: Any, expiry_seconds: Optional[int] = None) -> bool:
        """
        Store data in Redis cache asynchronously.
        
        Args:
            key: Cache key
            value: Data to be cached
            expiry_seconds: Optional TTL in seconds
            
        Returns:
            bool: Success status
        """
        try:
            # Serialize value to JSON if it's not a simple type
            if not isinstance(value, (str, int, float, bool)):
                value = json.dumps(value)
            
            # Store in Redis
            if expiry_seconds:
                return await self.redis_client.setex(key, expiry_seconds, value)
            else:
                return await self.redis_client.set(key, value)
        except Exception as e:
            print(f"Redis cache error: {str(e)}")
            return False
    
    async def get_cache(self, key: str, default_value: Any = None) -> Any:
        """
        Retrieve data from Redis cache asynchronously.
        
        Args:
            key: Cache key to retrieve
            default_value: Value to return if key doesn't exist
            
        Returns:
            Any: Cached data or default value
        """
        try:
            value = await self.redis_client.get(key)
            if value is None:
                return default_value
                
            # Try to decode and deserialize the value
            try:
                decoded_value = value.decode('utf-8')
                # Try to parse as JSON
                try:
                    return json.loads(decoded_value)
                except (json.JSONDecodeError, TypeError):
                    # If not JSON, return as is
                    return decoded_value
            except UnicodeDecodeError:
                # If binary data, return as is
                return value
        except Exception as e:
            print(f"Redis cache retrieval error: {str(e)}")
            return default_value
    
    async def delete_cache(self, key: str) -> bool:
        """
        Delete a key from Redis cache asynchronously.
        
        Args:
            key: Cache key to delete
            
        Returns:
            bool: Success status
        """
        try:
            return bool(await self.redis_client.delete(key))
        except Exception as e:
            print(f"Redis cache deletion error: {str(e)}")
            return False
    
    async def flush_all(self) -> bool:
        """
        Clear all cache asynchronously.
        
        Returns:
            bool: Success status
        """
        try:
            return await self.redis_client.flushall()
        except Exception as e:
            print(f"Redis flush error: {str(e)}")
            return False
            
    async def exists(self, key: str) -> bool:
        """
        Check if a key exists in Redis asynchronously.
        
        Args:
            key: Cache key to check
            
        Returns:
            bool: True if key exists, False otherwise
        """
        try:
            return bool(await self.redis_client.exists(key))
        except Exception as e:
            print(f"Redis exists check error: {str(e)}")
            return False
    
    async def get_ttl(self, key: str) -> int:
        """
        Get the remaining time to live for a key in seconds asynchronously.
        
        Args:
            key: Cache key
            
        Returns:
            int: TTL in seconds, -1 if key has no expiry, -2 if key doesn't exist
        """
        try:
            return await self.redis_client.ttl(key)
        except Exception as e:
            print(f"Redis TTL check error: {str(e)}")
            return -2
