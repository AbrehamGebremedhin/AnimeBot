import os
from neo4j import GraphDatabase, AsyncGraphDatabase
from dotenv import load_dotenv

# Load environment variables
load_dotenv(r'D:\Projects\AnimeBot\config.env')


class Neo4jConnection:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Neo4jConnection, cls).__new__(
                cls, *args, **kwargs)
            cls._instance._init_driver()
        return cls._instance

    def _init_driver(self):
        self.NEO4J_URI = os.getenv('NEO4J_URI')
        self.NEO4J_USERNAME = os.getenv('NEO4J_USERNAME')
        self.NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')
        self.AUTH = (self.NEO4J_USERNAME, self.NEO4J_PASSWORD)
        self.driver = GraphDatabase.driver(self.NEO4J_URI, auth=self.AUTH)
        self.async_driver = AsyncGraphDatabase.driver(
            self.NEO4J_URI, auth=self.AUTH)

    def get_driver(self):
        return self.driver

    def get_async_driver(self):
        return self.async_driver

    def close(self):
        if self.driver:
            self.driver.close()
        if self.async_driver:
            self.async_driver.close()

# Usage:
# connection = Neo4jConnection()
# driver = connection.get_driver()
# async_driver = connection.get_async_driver()
