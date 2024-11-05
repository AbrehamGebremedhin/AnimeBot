from neo4j import GraphDatabase
from chat_processor.mal_api import API_CALL

mal_api = API_CALL()


class UserService:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

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

    # 3. Update array (e.g., add genre to preferred_genres)
    def update_array(self, user_id, field_name, value, action="add"):
        query = f"""
        MATCH (u:User {{id: $user_id}})
        """
        if action == "add":
            query += f"SET u.{
                field_name} = coalesce(u.{field_name}, []) + $value RETURN u"
        elif action == "remove":
            query += f"SET u.{field_name} = [x IN u.{
                field_name} WHERE x <> $value] RETURN u"
        with self.driver.session() as session:
            result = session.run(query, user_id=user_id, value=value)
            return result.single()

    # 4. Update nested object (e.g., story_preferences)
    def update_nested_object(self, user_id, object_field, key, value):
        query = f"""
        MATCH (u:User {{id: $user_id}})
        SET u.{object_field}.{key} = $value
        RETURN u
        """
        with self.driver.session() as session:
            result = session.run(query, user_id=user_id, value=value)
            return result.single()
