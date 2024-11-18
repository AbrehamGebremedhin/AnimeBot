import os
from dotenv import load_dotenv
from neo4j import GraphDatabase
from .mal_api import API_CALL

mal_api = API_CALL()

load_dotenv(r'D:\Projects\AnimeBot\config.env')

class UserService:
    def __init__(self):
        self.driver = GraphDatabase.driver(os.getenv("NEO4J_URI"), auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD")))

    def close(self):
        self.driver.close()

    # 0. Create user
    def create_user(self, user_id, email, username):
        query = """
        CREATE (u:User {id: $user_id, email: $email, username: $username})
        RETURN u
        """
        with self.driver.session() as session:
            result = session.run(query, user_id=user_id, email=email, username=username)
            return result.single()

    # 1. Update relationship (e.g., favorite_anime)
    def update_relationship(self, user_id, relation_type, node_type, node_value):
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
        with self.driver.session() as session:
            result = session.run(query, user_id=user_id,
                                 related_node_id=related_node_id)
            return result.single()

    # 2. Update simple variable (e.g., age)
    def update_variable(self, user_id, field_name, value):
        query = f"""
        MATCH (u:User {{id: $user_id}})
        SET u.{field_name} = $value
        RETURN u
        """
        with self.driver.session() as session:
            result = session.run(query, user_id=user_id, value=value)
            return result.single()
        
    # 3. Get user profile
    def get_user_profile(self, user_id):
        query = """
        MATCH (u:User {id: $user_id})
        RETURN u
        """
        with self.driver.session() as session:
            result = session.run(query, user_id=user_id)
            return result.single()
