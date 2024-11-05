import os
import logging
import requests
from neo4j import GraphDatabase
from dotenv import load_dotenv
from langchain_community.embeddings import OllamaEmbeddings

# Load environment variables
load_dotenv(r'D:\Projects\AnimeBot\config.env')

# Initialize logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class API_CALL:
    def __init__(self):
        self.NEO4J_URI = os.getenv('NEO4J_URI')
        self.NEO4J_USERNAME = os.getenv('NEO4J_USERNAME')
        self.NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')
        self.AUTH = (self.NEO4J_USERNAME, self.NEO4J_PASSWORD)
        self.driver = GraphDatabase.driver(self.NEO4J_URI, auth=self.AUTH)
        self.embedder = OllamaEmbeddings(model="nomic-embed-text")

    def create_anime_text(self, row):
        genres = ", ".join([genre['name'] for genre in row['genres']])
        attributes = [
            f"anime name: {row['alternative_titles']['en']},",
            f"synopsis: {row['synopsis']},",
            f"type: {row['media_type']},",
            f"number of episodes: {row['num_episodes']},",
            f"aired: {row['start_date']} to {row['end_date']},",
            f"status: {row['status']},",
            f"source: {row['source']},",
            f"average show length: {(row['average_episode_duration'] / 60)},",
            f"anime is rated: {row['rating']},",
            f"the anime has a score of: {str(row['mean'])},",
            f"the genres the anime belongs to: {genres}"
        ]
        return " ".join(attributes)

    def anime_exists_name(self, anime_name):
        query = """
        MATCH (anime:Anime)
        WHERE toLower(anime.name) = toLower($anime_name)
        RETURN COUNT(anime) > 0 AS exists, elementId(anime) AS unique_id
        """

        with self.driver.session() as session:
            result = session.run(query, {'anime_name': anime_name})
            record = result.single()
            exists = record['exists']
            unique_id = record['unique_id'] if exists else None
            return exists, unique_id

    def genre_exists(self, genre_name):
        query = """
        MATCH (genre:Genre)
        WHERE toLower(genre.name) = toLower($genre)
        RETURN COUNT(genre) > 0 AS exists, elementId(genre) AS unique_id
        """

        with self.driver.session() as session:
            result = session.run(query, {'genre': genre_name})
            record = result.single()
            exists = record['exists']
            unique_id = record['unique_id'] if exists else None
            return exists, unique_id

    def embed_text(self, text):
        return self.embedder.embed_query(text)

    def get_data(self, anime_name):
        api_url = f"https://api.myanimelist.net/v2/anime?q={
            anime_name}&limit=1"
        response = requests.request('GET', api_url, headers={
            "X-MAL-CLIENT-ID": os.getenv('CLIENT_ID')
        })

        if response.status_code == 200:
            data = response.json()['data'][0]['node']
            anime_id = data['id']

            if self.anime_exists(anime_id):
                logging.info(f"Anime {anime_id} already exists. Skipping.")
                return

            try:
                embedding = self.embed_text(self.create_anime_text(data))
            except Exception as e:
                logging.error(f"Failed to embed text for anime {
                              anime_id}: {e}")
                return

            anime_var = f"anime_{anime_id}"
            queries = [
                (
                    f"MERGE ({
                        anime_var}:Anime {{anime_id: $id, name: $name, synopsis: $synopsis, "
                    f"type: $type, no_episodes: $no_episodes, aired: $aired, status: $status, "
                    f"duration: $duration, rating: $rating, score: $score, image_url: $image_url, "
                    f"embedding: $embedding}})",
                    {
                        'id': anime_id, 'name': data['alternative_titles']['en'], 'synopsis': data['synopsis'],
                        'type': data['media_type'], 'no_episodes': data['num_episodes'], 'aired': f"{data['start_date']} to {data['end_date']}",
                        'status': data['status'], 'duration': data['average_episode_duration'] / 60, 'rating': data['rating'],
                        'score': data['mean'], 'image_url': data['main_picture']['large'], 'embedding': embedding
                    }
                )
            ]

            genres = data['genres']
            genre_data = [{'anime_id': anime_id, 'genre': genre['name']}
                          for genre in genres]
            queries.append((
                f"UNWIND $batch as row MATCH (anime:Anime {{anime_id: row.anime_id}}) "
                f"MERGE (genre:Genre {{name: row.genre}}) MERGE (anime)-[:IN_GENRE]->(genre)",
                {'batch': genre_data}
            ))

            with self.driver.session() as session:
                for query, params in queries:
                    session.run(query, params)

            logging.info(f"Anime {anime_id} data loaded successfully.")

    def close(self):
        if self.driver:
            self.driver.close()
            logging.info("Neo4J driver closed")


api = API_CALL(anime_name='naruto')
print(api.genre_exists(genre_name='sports'))
api.close()
