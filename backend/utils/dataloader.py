import os
import logging
import pandas as pd
import concurrent.futures
import time
from neo4j import GraphDatabase
from dotenv import load_dotenv
from tqdm import tqdm
from langchain_community.embeddings import OllamaEmbeddings

load_dotenv(r'D:\Projects\AnimeBot\config.env')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class Neo4JDataloader:
    def __init__(self, checkpoint_file='checkpoint.txt', max_retries=3):
        self.NEO4J_URI = os.getenv('NEO4J_URI')
        self.NEO4J_USERNAME = os.getenv('NEO4J_USERNAME')
        self.NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')
        self.AUTH = (self.NEO4J_USERNAME, self.NEO4J_PASSWORD)
        self.driver = GraphDatabase.driver(
            self.NEO4J_URI, auth=self.AUTH, max_connection_lifetime=60)
        self.embedder = OllamaEmbeddings(model="nomic-embed-text")
        self.checkpoint_file = checkpoint_file
        self.max_retries = max_retries

        try:
            self.driver.verify_connectivity()
            logging.info("Successfully connected to Neo4J")
        except Exception as e:
            logging.error(f"Error connecting to Neo4J: {e}")
            self.close()

    @staticmethod
    def sanitize_string(input_string):
        replacements = {
            ' ': '_', '-': '_', '.': '_', '&': '_', '°': '_', '+': '', '(': '', ')': '',
        }
        for old, new in replacements.items():
            input_string = input_string.replace(old, new)
        return input_string

    def anime_exists(self, anime_id):
        query = "MATCH (anime:Anime {anime_id: $anime_id}) RETURN COUNT(anime) > 0 AS exists"
        with self.driver.session() as session:
            result = session.run(query, {'anime_id': anime_id})
            return result.single()['exists']

    def embed_text(self, text):
        for attempt in range(self.max_retries):
            try:
                return self.embedder.embed_query(text)
            except Exception as e:
                logging.warning(f"Embedding failed. Attempt {
                                attempt + 1} of {self.max_retries}. Error: {e}")
                time.sleep(2 ** attempt)
        raise Exception("Max retries exceeded for embedding")

    def batch_execute(self, queries):
        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                for query, params in queries:
                    tx.run(query, params)
                tx.commit()

    def get_last_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, 'r') as f:
                return int(f.read().strip())
        return 0

    def update_checkpoint(self, index):
        with open(self.checkpoint_file, 'w') as f:
            f.write(str(index))

    def create_anime_text(self, row):
        attributes = [
            f"anime name: {row['Name']},", f"synopsis: {row['Synopsis']},",
            f"type: {row['Type']},", f"number of episodes: {row['Episodes']},",
            f"aired: {row['Aired']},", f"status: {row['Status']},",
            f"source: {row['Source']},", f"average show length: {
                row['Duration']},",
            f"anime is rated: {row['Rating']},", f"the anime has a score of: {
                str(row['Score'])},",
            f"the genres the anime belongs to: {row['Genres']}"
        ]
        return " ".join(attributes)

    def process_data_in_batches(self, df):
        batch_queries = []
        node_count = 0
        relationship_count = 0
        last_checkpoint = self.get_last_checkpoint()

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            future_to_row = {
                executor.submit(self.embed_text, self.create_anime_text(row)): (index, row)
                for index, row in df.iterrows() if index >= last_checkpoint
            }

            with tqdm(total=len(df) - last_checkpoint, desc="Processing rows", unit="row") as pbar:
                for future in concurrent.futures.as_completed(future_to_row):
                    index, row = future_to_row[future]

                    if self.anime_exists(row['anime_id']):
                        logging.info(
                            f"Anime {row['anime_id']} already exists. Skipping.")
                        pbar.update(1)
                        continue

                    try:
                        embedding = future.result()
                    except Exception as e:
                        logging.error(f"Failed to embed row {index}: {e}")
                        continue

                    anime_var = f"anime_{row['anime_id']}"
                    queries = [
                        (
                            f"MERGE ({
                                anime_var}:Anime {{anime_id: $id, name: $name, synopsis: $synopsis, "
                            f"type: $type, no_episodes: $no_episodes, aired: $aired, status: $status, "
                            f"duration: $duration, rating: $rating, score: $score, image_url: $image_url, "
                            f"embedding: $embedding}})",
                            {
                                'id': row['anime_id'], 'name': row['Name'], 'synopsis': row['Synopsis'],
                                'type': row['Type'], 'no_episodes': row['Episodes'], 'aired': row['Aired'],
                                'status': row['Status'], 'duration': row['Duration'], 'rating': row['Rating'],
                                'score': row['Score'], 'image_url': row['Image URL'], 'embedding': embedding
                            }
                        )
                    ]

                    genres = row['Genres'].split(", ")
                    genre_data = [{'anime_id': row['anime_id'],
                                   'genre': genre} for genre in genres]
                    queries.append((
                        f"UNWIND $batch as row MATCH (anime:Anime {{anime_id: row.anime_id}}) "
                        f"MERGE (genre:Genre {{name: row.genre}}) MERGE (anime)-[:IN_GENRE]->(genre)",
                        {'batch': genre_data}
                    ))
                    relationship_count += len(genres)

                    # Other relationships
                    batch_queries.extend(queries)
                    node_count += 1

                    if len(batch_queries) >= 100:
                        self.batch_execute(batch_queries)
                        batch_queries = []

                    if index % 100 == 0:
                        self.update_checkpoint(index)

                    pbar.update(1)

        if batch_queries:
            self.batch_execute(batch_queries)

        logging.info(f"Created {node_count} nodes and {
                     relationship_count} relationships in Neo4j")

    def load_anime_data(self):
        df = pd.read_csv(
            r'D:\Projects\AnimeBot\backend\data\anime-dataset-cleaned.csv')
        self.process_data_in_batches(df)

    def close(self):
        if self.driver:
            self.driver.close()
            logging.info("Neo4J driver closed")


# Instantiate and run the Neo4JDataloader class
neo = Neo4JDataloader()
neo.load_anime_data()
neo.close()
