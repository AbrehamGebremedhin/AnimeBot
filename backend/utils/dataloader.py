import os
import logging
import pandas as pd
import asyncio
import aiohttp
import time  # For retry logic
from neo4j import AsyncGraphDatabase
from dotenv import load_dotenv
from tqdm.asyncio import tqdm
from langchain_community.embeddings import OllamaEmbeddings

# Load environment variables
load_dotenv(r'D:\Projects\AnimeBot\config.env')

# Initialize logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class AsyncNeo4JDataloader:
    def __init__(self, checkpoint_file='checkpoint.txt', max_retries=3):
        self.NEO4J_URI = os.getenv('NEO4J_URI')
        self.NEO4J_USERNAME = os.getenv('NEO4J_USERNAME')
        self.NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')
        self.AUTH = (self.NEO4J_USERNAME, self.NEO4J_PASSWORD)
        self.driver = AsyncGraphDatabase.driver(self.NEO4J_URI, auth=self.AUTH)
        self.embedder = OllamaEmbeddings(model="nomic-embed-text")
        self.checkpoint_file = checkpoint_file
        self.max_retries = max_retries

    @staticmethod
    def sanitize_string(input_string):
        replacements = {
            ' ': '_', '-': '_', '.': '_', '&': '_', '°': '_', '+': '', '(': '', ')': '',
        }
        for old, new in replacements.items():
            input_string = input_string.replace(old, new)
        return input_string

    async def anime_exists(self, anime_id):
        """Check if an anime with the given anime_id already exists in the database."""
        query = "MATCH (anime:Anime {anime_id: $anime_id}) RETURN COUNT(anime) > 0 AS exists"
        async with self.driver.session() as session:
            result = await session.run(query, {'anime_id': anime_id})
            return await result.single()['exists']

    async def embed_text(self, text):
        """Embed text and handle retries in case of failures."""
        for attempt in range(self.max_retries):
            try:
                return self.embedder.embed_query(text)
            except Exception as e:
                logging.warning(f"Embedding failed. Attempt {
                                attempt + 1} of {self.max_retries}. Error: {e}")
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
        raise Exception("Max retries exceeded for embedding")

    async def batch_execute(self, queries):
        """Execute Neo4j queries with session management."""
        async with self.driver.session() as session:
            async with session.begin_transaction() as tx:
                for query, params in queries:
                    await tx.run(query, params)
                await tx.commit()

    def get_last_checkpoint(self):
        """Read the last processed row index from the checkpoint file."""
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, 'r') as f:
                return int(f.read().strip())
        return 0

    def update_checkpoint(self, index):
        """Update the checkpoint file with the last processed row index."""
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

    async def process_data_in_batches(self, df):
        batch_queries = []
        node_count = 0
        relationship_count = 0
        last_checkpoint = self.get_last_checkpoint()

        tasks = []
        for index, row in df.iterrows():
            if index >= last_checkpoint:
                tasks.append(self.process_row(index, row, batch_queries))

        with tqdm(total=len(tasks), desc="Processing rows", unit="row") as pbar:
            for task in asyncio.as_completed(tasks):
                await task
                pbar.update(1)

        # Execute remaining queries
        if batch_queries:
            await self.batch_execute(batch_queries)

        logging.info(f"Created {node_count} nodes and {
                     relationship_count} relationships in Neo4j")

    async def process_row(self, index, row, batch_queries):
        """Process a single row from the dataframe asynchronously."""
        # Check if the anime already exists in the database
        if await self.anime_exists(row['anime_id']):
            logging.info(f"Anime {row['anime_id']} already exists. Skipping.")
            return

        try:
            embedding = await self.embed_text(self.create_anime_text(row))
        except Exception as e:
            logging.error(f"Failed to embed row {index}: {e}")
            return  # Skip this row and move on

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
        genre_data = [{'anime_id': row['anime_id'], 'genre': genre}
                      for genre in genres]
        queries.append((
            f"UNWIND $batch as row MATCH (anime:Anime {{anime_id: row.anime_id}}) "
            f"MERGE (genre:Genre {{name: row.genre}}) MERGE (anime)-[:IN_GENRE]->(genre)",
            {'batch': genre_data}
        ))

        # Add HAS_TYPE relationship
        queries.append((
            f"MATCH (anime:Anime {{anime_id: $anime_id}}) "
            f"MERGE (type:Type {{name: $type}}) "
            f"MERGE (anime)-[:HAS_TYPE]->(type)",
            {'anime_id': row['anime_id'], 'type': row['Type']}
        ))

        # Add SOURCED_FROM relationship
        queries.append((
            f"MATCH (anime:Anime {{anime_id: $anime_id}}) "
            f"MERGE (source:Source {{name: $source}}) "
            f"MERGE (anime)-[:SOURCED_FROM]->(source)",
            {'anime_id': row['anime_id'], 'source': row['Source']}
        ))

        # Add HAS_RATING_OF relationship
        queries.append((
            f"MATCH (anime:Anime {{anime_id: $anime_id}}) "
            f"MERGE (rating:Rating {{value: $rating}}) "
            f"MERGE (anime)-[:HAS_RATING_OF]->(rating)",
            {'anime_id': row['anime_id'], 'rating': row['Rating']}
        ))

        batch_queries.extend(queries)

        if len(batch_queries) >= 100:
            await self.batch_execute(batch_queries)
            batch_queries.clear()

        self.update_checkpoint(index)

    async def load_anime_data(self):
        df = pd.read_csv(
            r'D:\Projects\AnimeBot\backend\data\anime-dataset-cleaned.csv')
        await self.process_data_in_batches(df)

    def _create_user(self, tx, user_pk, user_name, email):
        query = (
            "MERGE (user:User {user_id: $user_pk}) "
            "SET user.name = $user_name, user.email = $email"
        )
        tx.run(query, user_pk=user_pk, user_name=user_name, email=email)

    async def close(self):
        if self.driver:
            await self.driver.close()
            logging.info("Neo4J driver closed")


# Instantiate and run the AsyncNeo4JDataloader class
async def main():
    loader = AsyncNeo4JDataloader()
    await loader.load_anime_data()
    await loader.close()

# Run the asyncio event loop
if __name__ == '__main__':
    asyncio.run(main())
