import os
import json
import redis.asyncio as redis
from dotenv import load_dotenv
from typing import Any, Dict, Optional, Union
import logging
import asyncio
import threading

logger = logging.getLogger(__name__)

class RedisService:
    """Service to handle Redis caching operations asynchronously."""
    
    _instance = None
    _initialized = False
    _lock = threading.RLock()  # Add thread lock for initialization
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:  # Thread-safe singleton creation
                if cls._instance is None:
                    cls._instance = super(RedisService, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize Redis connection using environment variables."""
        # Only run initialization once - with thread safety
        with self._lock:
            if RedisService._initialized:
                return
                
            load_dotenv()
            
            # Parse Redis endpoint
            redis_endpoint = os.getenv("REDIS_ENDPOINT", "")
            host, port = redis_endpoint.split(":") if ":" in redis_endpoint else (redis_endpoint, 18369)
            
            # Enhanced connection pool settings - fixed to use correct parameters
            connection_pool_kwargs = {
                "max_connections": 50,           # Maximum number of connections in pool
                "health_check_interval": 30,     # How often to check connection health
            }
            
            try:
                # Connect to Redis using async client with improved connection pooling
                self.redis_client = redis.Redis(
                    host=host,
                    port=int(port),
                    password=os.getenv("REDIS_PASSWORD", ""),
                    decode_responses=False,      # Keep binary data as is
                    socket_timeout=10,           # Socket timeout
                    socket_connect_timeout=10,   # Socket connection timeout
                    retry_on_timeout=True,       # Auto-retry on timeout
                    connection_pool=redis.ConnectionPool(
                        host=host,
                        port=int(port),
                        password=os.getenv("REDIS_PASSWORD", ""),
                        decode_responses=False,
                        **connection_pool_kwargs
                    )
                )
                
                # Cache lock for distributed locking
                self._locks = {}
                
                RedisService._initialized = True
                logger.info("Redis service initialized with enhanced connection pool")
            except Exception as e:
                logger.error(f"Failed to initialize Redis: {e}")
                # Create a fallback client with minimal settings if connection pool fails
                try:
                    self.redis_client = redis.Redis(
                        host=host,
                        port=int(port),
                        password=os.getenv("REDIS_PASSWORD", ""),
                        decode_responses=False,
                        socket_timeout=5
                    )
                    RedisService._initialized = True
                    logger.warning("Redis initialized with fallback settings")
                except Exception as e2:
                    logger.critical(f"Redis initialization failed completely: {e2}")
                    # Create a dummy client that logs operations but doesn't fail
                    self.redis_client = None
    
    async def _ensure_connection(self):
        """Ensure the Redis connection is healthy"""
        if not self.redis_client:
            logger.warning("Redis client is None, skipping operation")
            return False
            
        try:
            await self.redis_client.ping()
            return True
        except (redis.ConnectionError, redis.TimeoutError) as e:
            logger.warning(f"Redis connection error: {str(e)}. Reconnecting...")
            # Recreate the client
            load_dotenv()
            redis_endpoint = os.getenv("REDIS_ENDPOINT", "")
            host, port = redis_endpoint.split(":") if ":" in redis_endpoint else (redis_endpoint, 18369)
            
            try:
                self.redis_client = redis.Redis(
                    host=host,
                    port=int(port),
                    password=os.getenv("REDIS_PASSWORD", ""),
                    decode_responses=False,
                    socket_timeout=5
                )
                return True
            except Exception as e:
                logger.error(f"Redis reconnection failed: {e}")
                return False
    
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
        # Skip if Redis is not available
        if not self.redis_client:
            logger.warning(f"Redis not available, skipping set_cache for {key}")
            return False
            
        try:
            if not await self._ensure_connection():
                return False
            
            # Serialize value to JSON if it's not a simple type
            if not isinstance(value, (str, int, float, bool)):
                value = json.dumps(value)
            
            # Store in Redis
            if expiry_seconds:
                return await self.redis_client.setex(key, expiry_seconds, value)
            else:
                return await self.redis_client.set(key, value)
        except Exception as e:
            logger.error(f"Redis cache error: {str(e)}")
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
            await self._ensure_connection()
            
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
            logger.error(f"Redis cache retrieval error: {str(e)}")
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
            await self._ensure_connection()
            return bool(await self.redis_client.delete(key))
        except Exception as e:
            logger.error(f"Redis cache deletion error: {str(e)}")
            return False
    
    async def flush_all(self) -> bool:
        """
        Clear all cache asynchronously.
        
        Returns:
            bool: Success status
        """
        try:
            await self._ensure_connection()
            return await self.redis_client.flushall()
        except Exception as e:
            logger.error(f"Redis flush error: {str(e)}")
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
            await self._ensure_connection()
            return bool(await self.redis_client.exists(key))
        except Exception as e:
            logger.error(f"Redis exists check error: {str(e)}")
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
            await self._ensure_connection()
            return await self.redis_client.ttl(key)
        except Exception as e:
            logger.error(f"Redis TTL check error: {str(e)}")
            return -2
            
    async def acquire_lock(self, lock_name: str, timeout: int = 10) -> bool:
        """
        Acquire a distributed lock.
        
        Args:
            lock_name: Name of the lock to acquire
            timeout: Time in seconds the lock should be held
            
        Returns:
            bool: True if lock was acquired, False otherwise
        """
        try:
            await self._ensure_connection()
            
            lock_key = f"lock:{lock_name}"
            # Use NX option to only set if it doesn't exist
            result = await self.redis_client.set(
                lock_key, 
                "1", 
                ex=timeout, 
                nx=True
            )
            
            if result:
                # Store lock in instance for tracking
                if lock_name not in self._locks:
                    self._locks[lock_name] = asyncio.Lock()
                return True
            return False
        except Exception as e:
            logger.error(f"Redis lock acquisition error: {str(e)}")
            return False
    
    async def release_lock(self, lock_name: str) -> bool:
        """
        Release a distributed lock.
        
        Args:
            lock_name: Name of the lock to release
            
        Returns:
            bool: True if lock was released, False otherwise
        """
        try:
            await self._ensure_connection()
            
            lock_key = f"lock:{lock_name}"
            result = await self.redis_client.delete(lock_key)
            
            # Remove from instance tracking
            if lock_name in self._locks:
                del self._locks[lock_name]
                
            return bool(result)
        except Exception as e:
            logger.error(f"Redis lock release error: {str(e)}")
            return False
    
    async def close(self) -> None:
        """
        Close the Redis connection properly.
        
        This method should be called during application shutdown
        to ensure all Redis resources are properly released.
        """
        try:
            if self.redis_client:
                await self.redis_client.close()
                logger.info("Redis connection closed successfully")
        except Exception as e:
            logger.error(f"Error closing Redis connection: {str(e)}")
