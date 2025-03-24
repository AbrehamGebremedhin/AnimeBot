import os
from ..utils.neo4j_connection import Neo4jConnection
from ..utils.mal_api import API_CALL

mal_api = API_CALL()

class UserService:
    def __init__(self):
        # Use the singleton Neo4j connection
        self.neo4j_connection = Neo4jConnection()
        self.async_driver = self.neo4j_connection.get_async_driver()

    # 0. Create user
    async def create_user(self, user_id, username):
        query = """
        CREATE (u:User {id: $user_id, username: $username})
        RETURN u
        """
        async with self.async_driver.session() as session:
            result = await session.run(query, user_id=user_id, username=username)
            record = await result.single()
            return record

    # 1. Update relationship (e.g., favorite_anime)
    async def update_relationship(self, user_id, relation_type, node_type, node_value):
        related_node_id = None
        if node_type == "Anime":
            exists, related_node_id = mal_api.anime_exists_name(node_value)
            if not exists:
                mal_api.get_data(node_value)
                exists, related_node_id = mal_api.anime_exists_name(node_value)
        elif node_type == "Genre":
            exists, related_node_id = mal_api.genre_exists(node_value)
        query = f"""
        MATCH (u:User {{id: $user_id}})
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
        query = f"""
        MATCH (u:User {{id: $user_id}})
        SET u.{field_name} = $value
        RETURN u
        """
        async with self.async_driver.session() as session:
            result = await session.run(query, user_id=user_id, value=value)
            record = await result.single()
            return record
        
    # 3. Get user profile
    async def get_user_profile(self, user_id):
        query = """
        MATCH (u:User {id: $user_id})
        RETURN u
        """
        async with self.async_driver.session() as session:
            result = await session.run(query, user_id=user_id)
            record = await result.single()
            return record
