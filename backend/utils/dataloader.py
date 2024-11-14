import os
import logging
import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv
from tqdm.asyncio import tqdm
from langchain.schema import Document
from langchain_community.embeddings import OllamaEmbeddings

# Load environment variables
load_dotenv(r'D:\Projects\AnimeBot\config.env')

# Initialize logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class Neo4JDataloader:
    def __init__(self, checkpoint_file='checkpoint.txt', max_retries=3):
        self.NEO4J_URI = os.getenv('NEO4J_URI')
        self.NEO4J_USERNAME = os.getenv('NEO4J_USERNAME')
        self.NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')
        self.AUTH = (self.NEO4J_USERNAME, self.NEO4J_PASSWORD)
        self.driver = GraphDatabase.driver(self.NEO4J_URI, auth=self.AUTH)
        self.embedder = OllamaEmbeddings(model="nomic-embed-text")
        self.checkpoint_file = checkpoint_file
        self.max_retries = max_retries

        try:
            self.driver.verify_connectivity()
            logging.info("Successfully connected to Neo4J")
        except Exception as e:
            logging.error(f"Error connecting to Neo4J: {e}")
            self.close()


    def embed_text(self, text):
        return self.embedder.embed_documents(text)  # Correct method is embed_documents
    
    def create_anime_text(self, row):
        attributes = [
            f"anime name: {row['Name']},", f"synopsis: {row['Synopsis']},",
            f"type: {row['Type']},", f"number of episodes: {row['Episodes']},",
            f"aired: {row['Aired']},", f"status: {row['Status']},",
            f"source: {row['Source']},", f"average show length: {row['Duration']},",
            f"anime is rated: {row['Rating']},", f"the anime has a score of: {str(row['Score'])},",
            f"the genres the anime belongs to: {row['Genres']}"
        ]
        return " ".join(attributes)

    def create_embedded_csv(self, file_path):
        # Create a new column to store the embedded text
        df = pd.read_csv(file_path)
        df['embedded_text'] = None  # Initialize the column

        for index, row in df.iterrows():
            print(f"Processing row {index}...Of {len(df)}")
            text = str(self.create_anime_text(row))
            embedding = self.embed_text([Document(page_content=text)])
            
            # Ensure embedding is compatible with a single cell
            if isinstance(embedding, list) and len(embedding) == 1:
                embedding = embedding[0]  # Extract single embedding if wrapped in a list
            
            df.at[index, 'embedded_text'] = embedding

        # Save the modified DataFrame to CSV
        df.to_csv(r"D:\Projects\AnimeBot\backend\data\embedded_anime_dataset.csv", index=False)
    
    def batch_execute(self, queries):
        """Execute Neo4j queries with session management."""
        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                for query, params in queries:
                    tx.run(query, params)
                tx.commit()

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

    def process_data_in_batches(self, df, batch_size=100):
        batch_queries = []
        node_count, relationship_count = 0, 0

        last_checkpoint = self.get_last_checkpoint()

        with tqdm(total=len(df) - last_checkpoint, desc="Processing rows", unit="row") as pbar:
            for index, row in df.iterrows():
                if index < last_checkpoint:
                    continue

                # Anime node creation
                anime_var = f"anime_{row['anime_id']}"
                queries = [
                    (
                        f"MERGE ({anime_var}:Anime {{anime_id: $id, name: $name, synopsis: $synopsis, "
                        f"no_episodes: $no_episodes, aired: $aired, status: $status, duration: $duration, "
                        f"score: $score, image_url: $image_url}})",
                        {
                            'id': row['anime_id'], 'name': row['Name'], 'synopsis': row['Synopsis'],
                            'no_episodes': row['Episodes'], 'aired': row['Aired'], 'status': row['Status'],
                            'duration': row['Duration'], 'score': row['Score'], 'image_url': row['Image URL'], 'embedded_text': row['embedded_text']
                        }
                    )
                ]

                # Genre relationships
                genres = row['Genres'].split(", ")
                genre_data = [{'anime_id': row['anime_id'], 'genre': genre} for genre in genres]
                queries.append((
                    f"UNWIND $batch as row MATCH (anime:Anime {{anime_id: row.anime_id}}) "
                    f"MERGE (genre:Genre {{name: row.genre}}) MERGE (anime)-[:IN_GENRE]->(genre)",
                    {'batch': genre_data}
                ))
                relationship_count += len(genres)

                # Type relationship
                queries.append((
                    f"MATCH (anime:Anime {{anime_id: $anime_id}}) "
                    f"MERGE (type:Type {{name: $type}}) "
                    f"MERGE (anime)-[:HAS_TYPE]->(type)",
                    {'anime_id': row['anime_id'], 'type': row['Type']}
                ))

                # Source relationship
                queries.append((
                    f"MATCH (anime:Anime {{anime_id: $anime_id}}) "
                    f"MERGE (source:Source {{name: $source}}) "
                    f"MERGE (anime)-[:SOURCED_FROM]->(source)",
                    {'anime_id': row['anime_id'], 'source': row['Source']}
                ))

                # Rating relationship
                queries.append((
                    f"MATCH (anime:Anime {{anime_id: $anime_id}}) "
                    f"MERGE (rating:Rating {{name: $rating}}) "
                    f"MERGE (anime)-[:RATED_AS]->(rating)",
                    {'anime_id': row['anime_id'], 'rating': row['Rating']}
                ))

                batch_queries.extend(queries)
                node_count += 1

                if len(batch_queries) >= batch_size:
                    self.batch_execute(batch_queries)
                    batch_queries = []

                self.update_checkpoint(index)
                pbar.update(1)

        if batch_queries:
            self.batch_execute(batch_queries)

        logging.info(f"Created {node_count} nodes and {relationship_count} relationships in Neo4j")

    def load_anime_data(self):
        df = pd.read_csv(r'D:\Projects\AnimeBot\backend\data\embedded_anime_dataset.csv')
        self.process_data_in_batches(df)

    def close(self):
        if self.driver:
            self.driver.close()
            logging.info("Neo4J driver closed")

# Instantiate and run the Neo4JDataloader class
neo = Neo4JDataloader()
neo.load_anime_data()
neo.close()
