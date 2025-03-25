import os
import time
import logging
import sys
from neo4j import GraphDatabase, AsyncGraphDatabase
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv('.env')

class Neo4jConnection:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Neo4jConnection, cls).__new__(
                cls, *args, **kwargs)
            cls._instance._init_driver()
        return cls._instance

    def _init_driver(self, max_retries=5, retry_delay=5):
        """Initialize Neo4j drivers with retry logic"""
        print(os.getenv('NEO4J_URI'))
        self.NEO4J_URI = os.getenv('NEO4J_URI')
        self.NEO4J_USERNAME = os.getenv('NEO4J_USERNAME')
        self.NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')
        self.AUTH = (self.NEO4J_USERNAME, self.NEO4J_PASSWORD)
        
        # Connection pool settings for high concurrency
        self.max_connection_lifetime = 3600  # 1 hour
        self.max_connection_pool_size = 100  # Increased for high concurrency
        self.connection_acquisition_timeout = 60
        
        # Add additional connection pooling settings
        self.max_transaction_retry_time = 30.0
        self.connection_timeout = 30.0
        
        self.driver = None
        self.async_driver = None
        
        # Try to connect with retries
        for attempt in range(max_retries):
            try:
                logger.info(f"Connecting to Neo4j (attempt {attempt+1}/{max_retries})...")
                
                # Initialize the sync driver with connection pool settings
                self.driver = GraphDatabase.driver(
                    self.NEO4J_URI, 
                    auth=self.AUTH,
                    max_connection_lifetime=self.max_connection_lifetime,
                    max_connection_pool_size=self.max_connection_pool_size,
                    connection_acquisition_timeout=self.connection_acquisition_timeout,
                    max_transaction_retry_time=self.max_transaction_retry_time,
                    connection_timeout=self.connection_timeout
                )
                
                # Initialize the async driver with the same settings
                self.async_driver = AsyncGraphDatabase.driver(
                    self.NEO4J_URI, 
                    auth=self.AUTH,
                    max_connection_lifetime=self.max_connection_lifetime,
                    max_connection_pool_size=self.max_connection_pool_size,
                    connection_acquisition_timeout=self.connection_acquisition_timeout,
                    max_transaction_retry_time=self.max_transaction_retry_time,
                    connection_timeout=self.connection_timeout
                )
                
                # Verify connection
                self.driver.verify_connectivity()
                logger.info("Successfully connected to Neo4j database")
                break
            
            except Exception as e:
                logger.error(f"Connection attempt {attempt+1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error("All connection attempts failed")
                    raise Exception(f"Failed to connect to Neo4j after {max_retries} attempts: {str(e)}")

    def get_driver(self):
        """Get the synchronous driver, checking connection health first"""
        if not self.check_connection_health():
            self._init_driver()  # Reconnect if unhealthy
        return self.driver

    def get_async_driver(self):
        """Get the asynchronous driver, checking connection health first"""
        if not self.check_connection_health():
            self._init_driver()  # Reconnect if unhealthy
        return self.async_driver
    
    def check_connection_health(self):
        """Check if the connection to Neo4j is healthy"""
        try:
            if self.driver:
                self.driver.verify_connectivity()
                return True
        except Exception as e:
            logger.error(f"Connection health check failed: {str(e)}")
            return False
        return False

    def close(self):
        """Close all Neo4j driver connections"""
        try:
            if self.driver:
                self.driver.close()
                logger.info("Closed synchronous Neo4j driver")
                
            if self.async_driver:
                self.async_driver.close()
                logger.info("Closed asynchronous Neo4j driver")
                
        except Exception as e:
            logger.error(f"Error while closing Neo4j connections: {str(e)}")
