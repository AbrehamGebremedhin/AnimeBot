import os
import logging
from ..utils.neo4j_connection import Neo4jConnection
from ..utils.mal_api import API_CALL

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

mal_api = API_CALL()

class UserService:
    def __init__(self):
        # Use the singleton Neo4j connection
        self.neo4j_connection = Neo4jConnection()
        self.async_driver = self.neo4j_connection.get_async_driver()
    
    async def create_user(self, user_id, username):
        """
        Create a user node in Neo4j database
        
        Args:
            user_id: The user's ID
            username: The user's username
        """
        logger.info(f"Creating Neo4j user with ID: {user_id}, username: {username}")
        
        try:
            # Ensure user_id is treated as a string
            user_id = str(user_id)
            
            query = """
            MERGE (u:User {user_id: $user_id})
            ON CREATE SET u.username = $username, u.created_at = timestamp()
            ON MATCH SET u.username = $username, u.last_updated = timestamp()
            RETURN u
            """
            
            async with self.async_driver.session() as session:
                # Use user_id as string
                result = await session.run(query, 
                                        user_id=user_id, 
                                        username=username)
                record = await result.single()
                logger.info(f"Neo4j user created/updated successfully: {user_id}")
                return record
        except Exception as e:
            logger.error(f"Failed to create Neo4j user {user_id}: {str(e)}")
            # Re-raise the exception so it can be handled by the caller
            raise

    # 1. Update relationship (e.g., favorite_anime)
    async def update_relationship(self, user_id, relation_type, node_type, node_value):
        # Ensure user_id is treated as a string
        user_id = str(user_id)
        
        related_node_id = None
        if node_type == "Anime":
            exists, related_node_id = mal_api.anime_exists_name(node_value)
            if not exists:
                mal_api.get_data(node_value)
                exists, related_node_id = mal_api.anime_exists_name(node_value)
        elif node_type == "Genre":
            exists, related_node_id = mal_api.genre_exists(node_value)
        query = f"""
        MATCH (u:User {{user_id: $user_id}})
        MATCH (n {{id: $related_node_id}})
        MERGE (u)-[r:{relation_type}]->(n)
        RETURN u, r, n
        """
        async with self.async_driver.session() as session:
            result = await session.run(query, user_id=user_id,
                                 related_node_id=related_node_id)
            record = await result.single()
            return record

    # 2. Update simple variable (e.g., age)
    async def update_variable(self, user_id, field_name, value):
        # Ensure user_id is treated as a string
        user_id = str(user_id)
        
        query = f"""
        MATCH (u:User {{user_id: $user_id}})
        SET u.{field_name} = $value
        RETURN u
        """
        async with self.async_driver.session() as session:
            result = await session.run(query, user_id=user_id, value=value)
            record = await result.single()
            return record
        
    # 3. Get user profile
    async def get_user_profile(self, user_id):
        # Ensure user_id is treated as a string
        user_id = str(user_id)
        
        query = """
        MATCH (u:User {user_id: $user_id})
        RETURN u
        """
        try:
            async with self.async_driver.session() as session:
                result = await session.run(query, user_id=user_id)
                record = await result.single()
                
                if record:
                    # Convert Neo4j node to dictionary
                    user_data = dict(record["u"].items())
                    
                    # Add default empty list for preferred_genres if not present
                    if "preferred_genres" not in user_data:
                        user_data["preferred_genres"] = []
                    
                    return user_data
                else:
                    # Return a default profile if user not found
                    return {"user_id": user_id, "preferred_genres": []}
        except Exception as e:
            print(f"Error getting user profile: {str(e)}")
            # Return a default profile on error
            return {"user_id": user_id, "preferred_genres": []}
    
    async def user_exists(self, user_id):
        """
        Check if a user exists in the Neo4j database
        
        Args:
            user_id: The user's ID
            
        Returns:
            bool: True if user exists, False otherwise
        """
        # Ensure user_id is treated as a string
        user_id = str(user_id)
        
        query = """
        MATCH (u:User {user_id: $user_id})
        RETURN u
        """
        
        try:
            async with self.async_driver.session() as session:
                result = await session.run(query, user_id=user_id)
                record = await result.single()
                return record is not None
        except Exception as e:
            logger.error(f"Error checking if user exists in Neo4j: {str(e)}")
            # Re-raise to be handled by caller
            raise
