import os
import logging
import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv
from tqdm import tqdm  # Import tqdm for the progress bar
import concurrent.futures

# Load environment variables
load_dotenv(r'D:\Projects\AnimeBot\config.env')

# Initialize logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class Neo4JDataloader:
    def __init__(self):
        self.NEO4J_URI = os.getenv('NEO4J_URI')
        self.NEO4J_USERNAME = os.getenv('NEO4J_USERNAME')
        self.NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')
        self.AUTH = (self.NEO4J_USERNAME, self.NEO4J_PASSWORD)

        # Initialize the driver
        self.driver = GraphDatabase.driver(self.NEO4J_URI, auth=self.AUTH)

        # Verify connectivity
        try:
            self.driver.verify_connectivity()
            logging.info("Successfully connected to Neo4J")
        except Exception as e:
            logging.error(f"Error connecting to Neo4J: {e}")
            self.close()

    @staticmethod
    def sanitize_string(input_string):
        replacements = {
            ' ': '_',
            '-': '_',
            '.': '_',
            '&': '_',
            '°': '_',
            '+': '',
            '(': '',
            ')': '',
        }
        for old, new in replacements.items():
            input_string = input_string.replace(old, new)
        return input_string

    def load_anime_data(self):
        df = pd.read_csv(r'D:\Projects\AnimeBot\backend\data\anime-dataset-cleaned.csv')

        # Initialize counters for nodes and relationships
        node_count = 0
        relationship_count = 0

        def process_row(row):
            queries = []
            anime_var = f"anime_{row['anime_id']}"
            queries.append((
                f"MERGE ({anime_var}:Anime {{anime_id: $id, name: $name, synopsis: $synopsis, type: $type, no_episodes: $no_episodes, aired: $aired, status: $status, duration: $duration, rating: $rating, score: $score, image_url: $image_url}})",
                {
                    'id': row['anime_id'],
                    'name': row['Name'],
                    'synopsis': row['Synopsis'],
                    'type': row['Type'],
                    'no_episodes': row['Episodes'],
                    'aired': row['Aired'],
                    'status': row['Status'],
                    'duration': row['Duration'],
                    'rating': row['Rating'],
                    'score': row['Score'],
                    'image_url': row['Image URL']
                }
            ))

            genres = row['Genres'].split(", ")
            for genre in genres:
                genre_var = f"genre_{self.sanitize_string(genre)}"
                queries.append((
                    f"MERGE ({genre_var}:Genre {{name: $name}})", {'name': genre}
                ))
                queries.append((
                    f"""
                    MATCH (anime:Anime {{anime_id: $anime_id}})
                    MERGE (genre:Genre {{name: $genre}})
                    MERGE (anime)-[:IN_GENRE]->(genre)
                    """,
                    {'anime_id': row['anime_id'], 'genre': genre}
                ))

            source = row['Source']
            source_var = f"source_{self.sanitize_string(source)}"
            queries.append((
                f"MERGE ({source_var}:Source {{name: $name}})", {'name': source}
            ))
            queries.append((
                f"""
                MATCH (anime:Anime {{anime_id: $anime_id}})
                MERGE (source:Source {{name: $source}})
                MERGE (anime)-[:FROM_SOURCE]->(source)
                """,
                {'anime_id': row['anime_id'], 'source': source}
            ))

            return queries

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_row = {executor.submit(process_row, row): row for index, row in df.iterrows()}
            for future in tqdm(concurrent.futures.as_completed(future_to_row), total=len(df), desc="Loading data"):
                queries = future.result()
                with self.driver.session() as session:
                    for query, params in queries:
                        session.run(query, **params)
                        if "MERGE (anime)-[:IN_GENRE]->(genre)" in query or \
                           "MERGE (anime)-[:FROM_SOURCE" in query:
                            relationship_count += 1
                        else:
                            node_count += 1

        logging.info(f"Created {node_count} nodes and {relationship_count} relationships in the Neo4j database")

    def close(self):
        # Close the driver
        if self.driver:
            self.driver.close()
            logging.info("Neo4J driver closed")


# Instantiate and run the Neo4JDataloader class
neo = Neo4JDataloader()
neo.load_anime_data()
neo.close()
