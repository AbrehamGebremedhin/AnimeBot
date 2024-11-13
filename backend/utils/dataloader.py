import os
import logging
import pandas as pd
import asyncio
import aiohttp
import time  # For retry logic
from neo4j import AsyncGraphDatabase
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
        self.driver = AsyncGraphDatabase.driver(self.NEO4J_URI, auth=self.AUTH)
        self.embedder = OllamaEmbeddings(model="nomic-embed-text")
        self.checkpoint_file = checkpoint_file
        self.max_retries = max_retries

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

neo = Neo4JDataloader()
neo.create_embedded_csv(r"D:\Projects\AnimeBot\backend\data\anime-dataset-cleaned.csv")
