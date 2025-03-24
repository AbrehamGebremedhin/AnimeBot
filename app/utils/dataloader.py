import os
import json
import logging
import pandas as pd
import glob
import time
import backoff
import sys
from tqdm.asyncio import tqdm
from langchain.schema import Document
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from dotenv import load_dotenv

# Adjust import path to work when run directly
try:
    from app.utils.neo4j_connection import Neo4jConnection
except ModuleNotFoundError:
    # When running the script directly, adjust the path
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
    from app.utils.neo4j_connection import Neo4jConnection

# Load environment variables
load_dotenv(r'.env')

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Neo4JDataloader:
    def __init__(self, checkpoint_file='checkpoint.txt', embedding_checkpoint_file='embedding_checkpoint.txt', 
                 max_retries=3, retry_delay=5):
        # Use the Neo4jConnection singleton instead of creating a new connection
        self.connection = Neo4jConnection()
        self.driver = self.connection.get_driver()
        self.async_driver = self.connection.get_async_driver()
        
        # Check if Google API key is set
        google_api_key = os.getenv('GEMINI_API_KEY')
        if not google_api_key:
            logging.error("GEMINI_API_KEY environment variable not set")
            raise ValueError("GEMINI_API_KEY environment variable is required")
            
        # Initialize Google embeddings with explicit API key
        self.embedder = GoogleGenerativeAIEmbeddings(
            model="models/embedding-001",
            google_api_key=google_api_key,
        )
        
        self.checkpoint_file = checkpoint_file
        self.embedding_checkpoint_file = embedding_checkpoint_file
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        try:
            self.driver.verify_connectivity()
            logging.info("Successfully connected to Neo4J")
        except Exception as e:
            logging.error(f"Error connecting to Neo4J: {e}")

    # Decorator for retry logic on embedding operations
    @backoff.on_exception(backoff.expo, 
                         (Exception,), 
                         max_tries=3, 
                         giveup=lambda e: isinstance(e, ValueError),
                         on_backoff=lambda details: logger.info(f"Retrying embedding operation... (attempt {details['tries']})")
                         )
    def embed_text(self, text):
        """Embed text with retry logic for API failures"""
        if isinstance(text, list) and all(isinstance(item, Document) for item in text):
            text_content = [doc.page_content for doc in text]
            return self.embedder.embed_documents(text_content)
        return self.embedder.embed_documents(text)

    def create_anime_text(self, row):
        attributes = [
            f"anime name: {row.get('Name', '')},", 
            f"synopsis: {row.get('Synopsis', '')},",
            f"type: {row.get('Type', '')},", 
            f"number of episodes: {row.get('Episodes', 0)},",
            f"aired: {row.get('Aired', '')},", 
            f"status: {row.get('Status', '')},",
            f"source: {row.get('Source', '')},", 
            f"average show length: {row.get('Duration', '')},",
            f"anime is rated: {row.get('Rating', '')},", 
            f"the anime has a score of: {str(row.get('Score', 0))},",
            f"the genres the anime belongs to: {row.get('Genres', '')}"
        ]
        return " ".join(attributes)

    def get_embedding_checkpoint(self):
        """Read the last processed row index for embedding from the checkpoint file."""
        try:
            if os.path.exists(self.embedding_checkpoint_file):
                with open(self.embedding_checkpoint_file, 'r') as f:
                    return int(f.read().strip())
            return 0
        except Exception as e:
            logging.error(f"Error reading embedding checkpoint file: {e}")
            return 0

    def update_embedding_checkpoint(self, index):
        """Update the embedding checkpoint file with the last processed row index."""
        try:
            temp_file = f"{self.embedding_checkpoint_file}.tmp"
            with open(temp_file, 'w') as f:
                f.write(str(index))
            
            os.replace(temp_file, self.embedding_checkpoint_file)
            logging.info(f"Embedding checkpoint updated: Last processed index = {index}")
        except Exception as e:
            logging.error(f"Error updating embedding checkpoint file: {e}")

    def create_embedded_csv(self, file_path, batch_size=1000):
        """Process data in batches, create embeddings, and save checkpoint after each batch"""
        output_dir = r"D:\Projects\animebot-backend\data\embedded_batches"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        try:
            df = pd.read_csv(file_path)
            total_rows = len(df)
            
            last_processed = self.get_embedding_checkpoint()
            logging.info(f"Resuming embedding from checkpoint at row {last_processed}")
            
            for start_idx in range(last_processed, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx].copy()
                batch_df['embedded_text'] = None
                
                logging.info(f"Processing batch from row {start_idx} to {end_idx-1}")
                
                for idx, row in batch_df.iterrows():
                    retry_count = 0
                    while retry_count < self.max_retries:
                        try:
                            relative_idx = idx - start_idx
                            logging.info(f"Processing row {idx} of {total_rows} (batch position {relative_idx+1}/{len(batch_df)})")
                            
                            text = str(self.create_anime_text(row))
                            embedding = self.embed_text([Document(page_content=text)])
                            
                            if isinstance(embedding, list) and len(embedding) == 1:
                                embedding = embedding[0]
                            
                            batch_df.at[idx, 'embedded_text'] = json.dumps(embedding)
                            break
                        except Exception as e:
                            retry_count += 1
                            if retry_count < self.max_retries:
                                logging.error(f"Error processing row {idx} (attempt {retry_count}): {e}")
                                logging.info(f"Retrying in {self.retry_delay} seconds...")
                                time.sleep(self.retry_delay)
                            else:
                                logging.error(f"Failed to process row {idx} after {self.max_retries} attempts: {e}")
                                batch_df.at[idx, 'embedded_text'] = json.dumps([])
                
                try:
                    batch_file = os.path.join(output_dir, f"embedded_batch_{start_idx}_{end_idx}.csv")
                    batch_df.to_csv(batch_file, index=False)
                    logging.info(f"Saved batch to {batch_file}")
                    
                    self.update_embedding_checkpoint(end_idx)
                except Exception as e:
                    logging.error(f"Error saving batch file: {e}")
            
            self.merge_embedded_batches(output_dir)
        except Exception as e:
            logging.error(f"Error in create_embedded_csv: {e}")
            raise

    def merge_embedded_batches(self, batch_dir):
        """Merge all batch CSV files into a single file"""
        logging.info("Merging batch files into a single CSV...")
        
        batch_files = glob.glob(os.path.join(batch_dir, "embedded_batch_*.csv"))
        
        if not batch_files:
            logging.error("No batch files found to merge")
            return
        
        batch_files.sort(key=lambda x: int(x.split('_')[-2]))
        all_dfs = []
        
        for file in batch_files:
            df = pd.read_csv(file)
            all_dfs.append(df)
            logging.info(f"Added {file} to merge list")
        
        merged_df = pd.concat(all_dfs, ignore_index=True)
        
        output_file = r"D:\Projects\animebot-backend\data\embedded_anime_dataset.csv"
        merged_df.to_csv(output_file, index=False)
        logging.info(f"Successfully merged all batches into {output_file}")

    def batch_execute(self, queries):
        """Execute Neo4j queries with session management, retry logic, and error handling."""
        for attempt in range(self.max_retries):
            try:
                if not self.connection.check_connection_health():
                    logging.warning("Neo4j connection is unhealthy, reconnecting...")
                    self.driver = self.connection.get_driver()
                
                with self.driver.session() as session:
                    with session.begin_transaction() as tx:
                        for query, params in queries:
                            logging.info(f"Executing Query: {query} with {params}")
                            tx.run(query, params)
                        tx.commit()
                return True
            except Exception as e:
                logging.error(f"Error executing batch (attempt {attempt+1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    logging.info(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logging.error("Max retries reached. Operation failed.")
                    raise

    def get_last_checkpoint(self):
        """Read the last processed row index from the checkpoint file."""
        try:
            if os.path.exists(self.checkpoint_file):
                with open(self.checkpoint_file, 'r') as f:
                    return int(f.read().strip())
            return 0
        except Exception as e:
            logging.error(f"Error reading checkpoint file: {e}")
            return 0

    def update_checkpoint(self, index):
        """Update the checkpoint file with the last processed row index."""
        try:
            temp_file = f"{self.checkpoint_file}.tmp"
            with open(temp_file, 'w') as f:
                f.write(str(index))
            
            os.replace(temp_file, self.checkpoint_file)
            logging.info(f"Checkpoint updated: Last processed index = {index}")
        except Exception as e:
            logging.error(f"Error updating checkpoint file: {e}")

    def process_data_in_batches(self, df, batch_size=100):
        batch_queries = []
        node_count, relationship_count = 0, 0
        last_checkpoint = self.get_last_checkpoint()
        logging.info(f"Resuming from checkpoint at row {last_checkpoint}")

        try:
            with tqdm(total=len(df) - last_checkpoint, desc="Processing rows", unit="row") as pbar:
                for index, row in df.iterrows():
                    if index < last_checkpoint:
                        continue

                    try:
                        embedded_text = json.loads(row.get('embedded_text', "[]"))
                        if not isinstance(embedded_text, list):
                            raise ValueError("embedded_text is not a list")
                        embedded_text = list(map(float, embedded_text))
                    except (json.JSONDecodeError, TypeError, ValueError) as e:
                        logging.error(f"Error processing embedded_text for row {index}: {e}")
                        embedded_text = None

                    anime_var = f"anime_{row.get('anime_id', index)}"
                    queries = [
                        (
                            f"MERGE ({anime_var}:Anime {{anime_id: $id, name: $name, synopsis: $synopsis, "
                            f"no_episodes: $no_episodes, aired: $aired, status: $status, duration: $duration, "
                            f"score: $score, image_url: $image_url, embedded_text: $embedded_text}})",
                            {
                                'id': row.get('anime_id', index), 
                                'name': row.get('Name', ""), 
                                'synopsis': row.get('Synopsis', ""),
                                'no_episodes': row.get('Episodes', 0), 
                                'aired': row.get('Aired', ""), 
                                'status': row.get('Status', ""),
                                'duration': row.get('Duration', ""), 
                                'score': row.get('Score', 0.0), 
                                'image_url': row.get('Image URL', ""),
                                'embedded_text': json.dumps(embedded_text) if embedded_text else "[]"
                            }
                        )
                    ]

                    # Process genre relationships
                    genres = row.get('Genres', "").split(", ")
                    genre_data = [{'anime_id': row.get('anime_id', index), 'genre': genre} for genre in genres if genre]
                    if genre_data:
                        queries.append((
                            f"UNWIND $batch AS row MATCH (anime:Anime {{anime_id: row.anime_id}}) "
                            f"MERGE (genre:Genre {{name: row.genre}}) MERGE (anime)-[:IN_GENRE]->(genre)",
                            {'batch': genre_data}
                        ))
                        relationship_count += len(genres)
                    
                    # Process Type relationship (TV, Movie, OVA, etc.)
                    anime_type = row.get('Type', "")
                    if anime_type:
                        queries.append((
                            f"MATCH (anime:Anime {{anime_id: $anime_id}}) "
                            f"MERGE (type:Type {{name: $type}}) MERGE (anime)-[:IS_TYPE]->(type)",
                            {'anime_id': row.get('anime_id', index), 'type': anime_type}
                        ))
                        relationship_count += 1
                    
                    # Process Source Material relationship
                    source = row.get('Source', "")
                    if source and source != "Unknown" and source != "Original":
                        queries.append((
                            f"MATCH (anime:Anime {{anime_id: $anime_id}}) "
                            f"MERGE (source:Source {{name: $source}}) MERGE (anime)-[:ADAPTED_FROM]->(source)",
                            {'anime_id': row.get('anime_id', index), 'source': source}
                        ))
                        relationship_count += 1
                    
                    # Process rating as a node for easier filtering
                    rating = row.get('Rating', "")
                    if rating:
                        queries.append((
                            f"MATCH (anime:Anime {{anime_id: $anime_id}}) "
                            f"MERGE (rating:Rating {{name: $rating}}) MERGE (anime)-[:HAS_RATING]->(rating)",
                            {'anime_id': row.get('anime_id', index), 'rating': rating}
                        ))
                        relationship_count += 1

                    batch_queries.extend(queries)
                    node_count += 1

                    if len(batch_queries) >= batch_size:
                        success = self.batch_execute(batch_queries)
                        if success:
                            batch_queries = []
                            self.update_checkpoint(index)

                    pbar.update(1)

            # Process any remaining queries with enhanced error handling
            if batch_queries:
                logging.info(f"Processing {len(batch_queries)} remaining queries...")
                
                # Try with increased retries for the final batch
                remaining_retries = self.max_retries * 2
                
                for attempt in range(remaining_retries):
                    try:
                        if not self.connection.check_connection_health():
                            logging.warning("Neo4j connection is unhealthy before processing remaining queries, reconnecting...")
                            self.driver = self.connection.get_driver()
                        
                        with self.driver.session() as session:
                            with session.begin_transaction() as tx:
                                for i, (query, params) in enumerate(batch_queries):
                                    try:
                                        logging.info(f"Executing remaining query {i+1}/{len(batch_queries)}")
                                        tx.run(query, params)
                                    except Exception as query_error:
                                        logging.error(f"Error executing query {i+1}: {query_error}")
                                        # Continue with other queries even if one fails
                                        continue
                                tx.commit()
                        
                        logging.info("Successfully processed all remaining queries")
                        self.update_checkpoint(index)
                        break
                    except Exception as e:
                        logging.error(f"Error executing remaining queries (attempt {attempt+1}/{remaining_retries}): {e}")
                        if attempt < remaining_retries - 1:
                            delay = self.retry_delay * (2 ** attempt)
                            logging.info(f"Retrying remaining queries in {delay} seconds...")
                            time.sleep(delay)
                        else:
                            logging.error("Max retries reached for remaining queries. Some data may not have been uploaded.")
                            # Still update the checkpoint to avoid reprocessing the same data
                            self.update_checkpoint(index)

            logging.info(f"Created {node_count} nodes and {relationship_count} relationships in Neo4j")
        except Exception as e:
            logging.error(f"Error in process_data_in_batches: {e}")
            raise

    def load_anime_data(self):
        df = pd.read_csv(r'D:\Projects\animebot-backend\data\embedded_anime_dataset.csv')
        self.process_data_in_batches(df)

    def close(self):
        # Don't close the Neo4j connection as it's managed by the singleton
        logging.info("Finished using Neo4J driver")
