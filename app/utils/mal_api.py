import os
import logging
import asyncio
import aiohttp
import sys
from dotenv import load_dotenv

# Adjust import path to work when run directly
try:
    from app.utils.neo4j_connection import Neo4jConnection
except ModuleNotFoundError:
    # When running the script directly, adjust the path
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
    from app.utils.neo4j_connection import Neo4jConnection

from langchain_google_genai import GoogleGenerativeAIEmbeddings

# Load environment variables
load_dotenv(r'D:\Projects\AnimeBot\config.env')

# Initialize logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class API_CALL:
    def __init__(self):
        # Use the Neo4j connection singleton
        self.neo4j_connection = Neo4jConnection()

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
        # Create a shared session for HTTP requests
        self.http_session = None
        self.client_id = os.getenv('CLIENT_ID')

    async def setup(self):
        """Initialize HTTP session for reuse"""
        if not self.http_session:
            self.http_session = aiohttp.ClientSession()

    async def close_http_session(self):
        """Close the HTTP session"""
        if self.http_session:
            await self.http_session.close()
            self.http_session = None

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

    async def entity_exists(self, entity_type, value, id_field=None):
        """
        Consolidated function to check if any entity exists in Neo4j.
        
        Args:
            entity_type (str): The label of the node (Anime, Genre, Type, Source, Rating)
            value (str or int): The value to check for
            id_field (str, optional): The field name to check against (default: based on entity_type)
        
        Returns:
            tuple: (exists (bool), unique_id (str or None))
        """
        # Set default id_field based on entity_type if not provided
        if id_field is None:
            if entity_type == "Anime":
                # Check if value is an integer (anime_id) or string (name)
                id_field = "anime_id" if isinstance(value, int) else "name"
            else:
                id_field = "name"  # Default for Genre, Type, Source, Rating
        
        # Build the query based on entity_type and id_field
        if id_field == "name":
            # Case-insensitive name comparison
            query = f"""
            MATCH (e:{entity_type})
            WHERE toLower(e.{id_field}) = toLower($value)
            RETURN COUNT(e) > 0 AS exists, elementId(e) AS unique_id
            """
        else:
            # Exact match for IDs
            query = f"""
            MATCH (e:{entity_type} {{{id_field}: $value}})
            RETURN COUNT(e) > 0 AS exists, elementId(e) AS unique_id
            """

        try:
            async_driver = self.neo4j_connection.get_async_driver()
            async with async_driver.session() as session:
                result = await session.run(query, {'value': value})
                record = await result.single()
                
                # Check if record is None (no results returned)
                if record is None:
                    logging.warning(f"No results found for {entity_type} with {id_field}={value}")
                    return False, None
                    
                exists = record['exists']
                unique_id = record['unique_id'] if exists else None
                return exists, unique_id
        except Exception as e:
            logging.error(f"Error checking if {entity_type} exists: {e}")
            return False, None

    async def anime_exists_name(self, anime_name):
        return await self.entity_exists("Anime", anime_name, "name")
        
    async def anime_exists(self, anime_id):
        return (await self.entity_exists("Anime", anime_id, "anime_id"))[0]

    async def genre_exists(self, genre_name):
        return await self.entity_exists("Genre", genre_name)

    async def type_exists(self, type_name):
        return await self.entity_exists("Type", type_name)
        
    async def source_exists(self, source_name):
        return await self.entity_exists("Source", source_name)
        
    async def rating_exists(self, rating_name):
        return await self.entity_exists("Rating", rating_name)

    async def anime_data(self, anime_name):
        await self.setup()
        api_url = f"https://api.myanimelist.net/v2/anime?q={anime_name}&limit=1&fields=synopsis"
        
        async with self.http_session.get(
            api_url, 
            headers={"X-MAL-CLIENT-ID": self.client_id}
        ) as response:
            if response.status == 200:
                data = await response.json()
                if 'data' in data and len(data['data']) > 0:
                    node = data['data'][0]['node']
                    url = node['main_picture']['large']
                    synopsis = node['synopsis']
                    return url, synopsis
            return None, None

    def embed_text(self, text):
        return self.embedder.embed_query(text)

    async def get_data(self, anime_name):
        await self.setup()
        api_url = f"https://api.myanimelist.net/v2/anime?q={anime_name}&limit=1&fields=id,alternative_titles,synopsis,media_type,num_episodes,start_date,end_date,status,genres,mean,rating,average_episode_duration,main_picture"
        
        async with self.http_session.get(
            api_url, 
            headers={"X-MAL-CLIENT-ID": self.client_id}
        ) as response:
            if response.status != 200:
                logging.error(f"API request failed with status {response.status}")
                return

            data = await response.json()
            if not data.get('data') or len(data['data']) == 0:
                logging.warning(f"No data found for anime '{anime_name}'")
                return

            node_data = data['data'][0]['node']
            anime_id = node_data['id']

            if await self.anime_exists(anime_id):
                logging.info(f"Anime {anime_id} already exists. Skipping.")
                return

            try:
                embedding = self.embed_text(self.create_anime_text(node_data))
            except Exception as e:
                logging.error(f"Failed to embed text for anime {anime_id}: {e}")
                return

            anime_var = f"anime_{anime_id}"
            create_anime_query = (
                f"MERGE ({anime_var}:Anime {{anime_id: $id, name: $name, synopsis: $synopsis, "
                f"type: $type, no_episodes: $no_episodes, aired: $aired, status: $status, "
                f"duration: $duration, rating: $rating, score: $score, image_url: $image_url, "
                f"embedding: $embedding}})"
            )
            
            anime_params = {
                'id': anime_id, 
                'name': node_data['alternative_titles']['en'], 
                'synopsis': node_data['synopsis'],
                'type': node_data['media_type'], 
                'no_episodes': node_data['num_episodes'], 
                'aired': f"{node_data['start_date']} to {node_data['end_date']}",
                'status': node_data['status'], 
                'duration': node_data['average_episode_duration'] / 60, 
                'rating': node_data['rating'],
                'score': node_data['mean'], 
                'image_url': node_data['main_picture']['large'], 
                'embedding': embedding
            }

            genres = node_data['genres']
            genre_data = [{'anime_id': anime_id, 'genre': genre['name']} for genre in genres]
            
            link_genres_query = (
                "UNWIND $batch as row "
                "MATCH (anime:Anime {anime_id: row.anime_id}) "
                "MERGE (genre:Genre {name: row.genre}) "
                "MERGE (anime)-[:IN_GENRE]->(genre)"
            )

            async_driver = self.neo4j_connection.get_async_driver()
            async with async_driver.session() as session:
                # Execute queries in parallel using transactions
                async with session.begin_transaction() as tx:
                    await tx.run(create_anime_query, anime_params)
                    await tx.run(link_genres_query, {'batch': genre_data})

            logging.info(f"Anime {anime_id} data loaded successfully.")

    async def close(self):
        # Only close the HTTP session, Neo4j connection is managed by the singleton
        await self.close_http_session()
        logging.info("API_CALL resources released")

# Example usage
async def example_usage():
    """
    Example demonstrating how to use the API_CALL class
    """
    # Initialize the API client
    api_client = API_CALL()
    
    try:
        # Example 1: Check if entities exist
        anime_name = "Cowboy Bebop"
        exists, anime_id = await api_client.anime_exists_name(anime_name)
        print(f"Anime '{anime_name}' exists: {exists}, ID: {anime_id}")
        
        # Example 2: Check various entity types
        genre_exists, genre_id = await api_client.genre_exists("Action")
        print(f"'Action' genre exists: {genre_exists}, ID: {genre_id}")
        
        type_exists, type_id = await api_client.type_exists("TV")
        print(f"'TV' type exists: {type_exists}, ID: {type_id}")
        
        # Example 3: Get anime data
        url, synopsis = await api_client.anime_data("Naruto")
        if url and synopsis:
            print(f"Found Naruto - Image URL: {url[:50]}...")
            print(f"Synopsis: {synopsis[:100]}...")
        else:
            print("Anime not found.")
        
        # Example 4: Load full anime data into Neo4j
        print("Loading full anime data for 'One Piece'...")
        await api_client.get_data("One Piece")
        
        # Example 5: Use the consolidated entity_exists function directly
        exists, entity_id = await api_client.entity_exists("Rating", "PG-13")
        print(f"'PG-13' rating exists: {exists}, ID: {entity_id}")
    
    finally:
        # Always close the connection
        await api_client.close()

if __name__ == "__main__":
    # Run the example
    asyncio.run(example_usage())
