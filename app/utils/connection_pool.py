import asyncio
import logging
from typing import Dict, Any, Optional
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

class ConnectionPool:
    """
    Generic connection pool for managing database connections
    """
    def __init__(
        self, 
        connection_factory, 
        max_connections: int = 10,
        connection_timeout: float = 30.0,
        idle_timeout: float = 300.0
    ):
        self.connection_factory = connection_factory
        self.max_connections = max_connections
        self.connection_timeout = connection_timeout
        self.idle_timeout = idle_timeout
        
        self._available = asyncio.Queue()
        self._in_use = set()
        self._connection_count = 0
        self._lock = asyncio.Lock()
        
    async def get_connection(self) -> Any:
        """Get a connection from the pool or create a new one"""
        # Try to get a connection from the pool
        try:
            connection = await asyncio.wait_for(
                self._available.get(), 
                timeout=self.connection_timeout
            )
            self._in_use.add(connection)
            return connection
        except asyncio.TimeoutError:
            # If no connection is available within timeout, try to create a new one
            async with self._lock:
                if self._connection_count < self.max_connections:
                    connection = await self.connection_factory()
                    self._connection_count += 1
                    self._in_use.add(connection)
                    return connection
                else:
                    # If maximum connections reached, wait for an available connection
                    connection = await self._available.get()
                    self._in_use.add(connection)
                    return connection
    
    async def release(self, connection: Any) -> None:
        """Release a connection back to the pool"""
        if connection in self._in_use:
            self._in_use.remove(connection)
            await self._available.put(connection)
    
    @asynccontextmanager
    async def connection(self):
        """Context manager for automatic connection acquisition and release"""
        conn = await self.get_connection()
        try:
            yield conn
        finally:
            await self.release(conn)
    
    async def close_all(self) -> None:
        """Close all connections in the pool"""
        # Close all available connections
        while not self._available.empty():
            connection = await self._available.get()
            await self._close_connection(connection)
        
        # Close all in-use connections
        for connection in list(self._in_use):
            await self._close_connection(connection)
            self._in_use.remove(connection)
        
        self._connection_count = 0
    
    async def _close_connection(self, connection: Any) -> None:
        """Close a single connection, with error handling"""
        try:
            if hasattr(connection, 'close'):
                if asyncio.iscoroutinefunction(connection.close):
                    await connection.close()
                else:
                    connection.close()
        except Exception as e:
            logger.error(f"Error closing connection: {str(e)}")
