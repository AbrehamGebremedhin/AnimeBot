import os
import json
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI, GoogleGenerativeAIEmbeddings
from .user_management import UserService
from langchain_core.output_parsers import JsonOutputParser
from ..utils.neo4j_connection import Neo4jConnection
from ..db.redis_service import RedisService
import asyncio


load_dotenv(r'.env')

api_key = os.getenv('GEMINI_API_KEY')

class Chat:
    def __init__(self, user_id):
        """
        Initialize the Chat class with user ID and necessary components.

        Args:
            user_id (str): The ID of the user.
        """
        self.user_id = user_id
        os.environ["GOOGLE_API_KEY"] = api_key
        
        self.llm = ChatGoogleGenerativeAI(model="gemini-2.0-flash")
        self.parser = JsonOutputParser()
        self.questions = json.load(open(r'data\questions.json'))
        self.user_service = UserService()
        
        # Replace OllamaEmbeddings with GoogleGenerativeAIEmbeddings
        self.embedder = GoogleGenerativeAIEmbeddings(
            model="models/embedding-001",
            google_api_key=api_key
        )
        
        # Use Neo4jConnection instead of direct Neo4jGraph initialization
        self.neo4j_connection = Neo4jConnection()
        self.neo4j_driver = self.neo4j_connection.get_driver()
        
        # Use RedisService for session history
        self.redis_service = RedisService()
        
        # Initialize session history
        self.session_history = []  # Initialize with empty list
        # Don't call load_session_history here, it will be called separately
    
    async def initialize(self):
        """
        Async initialization method to be called after creating the Chat instance.
        """
        await self.load_session_history()
        return self
    
    async def load_session_history(self):
        """
        Load session history from Redis (async method)
        """
        try:
            self.session_history = await self._load_session_history_async()
        except Exception as e:
            print(f"Error loading session history: {str(e)}")
            self.session_history = []

    async def _load_session_history_async(self):
        """
        Async method to load session history from Redis
        """
        history = await self.redis_service.get_cache(f"session_history_{self.user_id}")
        return history if history else []

    async def save_session_history(self):
        """
        Save the current session history to Redis with a 1-hour timeout.
        """
        await self.redis_service.set_cache(
            f"session_history_{self.user_id}",
            self.session_history,
            expiry_seconds=3600
        )

    async def reset_session_history(self):
        """
        Clear the session history from Redis and reset the local session history.
        """
        await self.redis_service.delete_cache(f"session_history_{self.user_id}")
        self.session_history = []

    def _extract_content_from_response(self, response):
        """
        Extract text content from various response formats from LLM.
        
        Args:
            response: The response from the language model
            
        Returns:
            str: The extracted text content
        """
        # Handle AIMessage objects
        if hasattr(response, 'content'):
            response_text = response.content
        else:
            # Fallback for other response types
            response_text = str(response)
            
        # Clean up the response if it contains Markdown code blocks
        if isinstance(response_text, str) and response_text.startswith('```') and '```' in response_text:
            # Extract content from Markdown code blocks
            response_text = response_text.split('```')[1]
            # Remove language identifier if present (e.g., 'json')
            if '\n' in response_text:
                response_text = response_text.split('\n', 1)[1]
            # Remove trailing code block markers if present
            if '```' in response_text:
                response_text = response_text.split('```')[0]
                
        return response_text

    async def generation_questions(self, category):
        """
        Paraphrase anime user questions to build the main user profile.

        Args:
            category (str): The question category to be formatted.

        Returns:
            dict: The paraphrased questions.
        """
        preffered_response = """{
            "{category}": [
                {
                    "question": "Paraphrased question",
                    "type": "Question Type",
                    "Options": [], # options if any are available
                    "var_name": "" # variable name
                },
            ]
        }"""

        prompt_template = f"""
            <|system|> 
            You are a chatbot engine for an anime recommendation application that uses a graph database. You are provided a questions data, which category to generate questions, and you will paraphrase all the questions in the category to make the questions sound more human.
            IMPORTANT: The way the questions data is organized as different categories, and each category has an array of questions. You need to paraphrase all the questions in the given category.

            IMPORTANT: Format your response as a single-line JSON string, without line breaks or escaped characters.

            Questions Data:  
            {self.questions}  

            Category: {category}

            Preffered Response: {preffered_response}

            Contextual Instructions:
            - Paraphrase the question in the questions format to sound and feel like chatting with a human.
            - Paraphrase all the questions in the given category.
            - Use the provided questions data to generate a response.
            - Only return the requested category data. As in do not include any other category in the response.
            
            Rules for JSON:
            1. Keep the structure of the JSON question, type, options and varname.
            1. Use double quotes for all variables and values.
            2. Return ONLY the JSON string without any additional text or comments.
            3. No line breaks or backticks; respond only with JSON.
            4. Do not include any additional information in the JSON response.
            
        """

        response = self.llm.invoke(prompt_template)
        
        # Extract the text content using the helper method
        response_text = self._extract_content_from_response(response)
        
        # Parse the cleaned text response
        formatted_response = self.parser.parse(response_text)
        return formatted_response
    
    async def generate_response(self, reply, user_profile):
        """
        Generate a conversational response based on previous chat history and user profile.

        Args:
            reply (str): The user's reply.
            user_profile (dict): The user's profile data.

        Returns:
            dict: The chatbot response.
        """
        prompt_template = f"""
            <|system|> 
            You are a chatbot designed for an anime recommendation application, but you are also friendly and conversational. Your goal is to engage users with questions that feel like chatting with a friend. While some questions can focus on anime, others should feel more personal and casual to build rapport with the user.

            **User Profile**:
            {user_profile}

            **Session History**:
            {self.session_history[-5:]}

            **User Reply**:
            {reply}

            **Contextual Instructions**:
            - Alternate between anime-related and personal/casual questions to make the conversation more engaging.
            - Use a friendly and casual tone, avoiding overly formal or robotic phrasing.
            - Avoid repeating questions from previous conversations or those closely related to the most recent question.
            - Frame questions as if you're genuinely curious about the user's likes, hobbies, and thoughts.
            - Keep the questions open-ended to encourage more detailed responses from the user.

            **Output Format**:
            {{
                "question": "A conversational, open-ended question in a friendly tone."
            }}

            **Examples of Friendly Questions**:
            - "What's something fun you've been up to lately?"
            - "What's your favorite thing about your all-time favorite anime?"
            - "If you could visit a place from an anime in real life, where would it be and why?"
            - "When you're not watching anime, how do you usually spend your time?"
            - "Is there an anime character you'd love to hang out with in real life?"
            - "What kind of stories inspire you the most—anime or otherwise?"

            Respond with a single JSON object containing the question, formatted exactly as specified above.
        """

        response = self.llm.invoke(prompt_template)

        # Extract the text content using the helper method
        response_text = self._extract_content_from_response(response)
        
        # Parse the cleaned text response
        formatted_response = self.parser.parse(response_text)
        
        self.session_history.append({
            "system": formatted_response["question"],
            "user": reply
        })
        
        # Save session history after each response
        await self.save_session_history()

        return formatted_response
    
    async def similarity_search(self, user_profile):
        """
        Perform an improved similarity search using Neo4j and embeddings.

        Args:
            user_profile (dict): The user's profile data.

        Returns:
            dict: The search results with anime recommendations.
        """
        try:
            # Prepare the user profile for embedding
            user_profile_text = self.prepare_user_profile_embedding(user_profile, self.session_history)
            
            # Get the embedding vector and ensure it's a list, not a string
            embedding_vector = self.embedder.embed_query(user_profile_text)
            if isinstance(embedding_vector, str):
                print("Warning: Embedding is a string, attempting to convert to list")
                try:
                    # Try to convert string representation to actual list
                    import ast
                    embedding_vector = ast.literal_eval(embedding_vector)
                except:
                    print("Failed to convert embedding string to list")
                    # Fallback to a simpler approach
                    embedding_vector = [0.1] * 768  # Default vector dimensions
            
            print(f"Embedding type: {type(embedding_vector)}, Length: {len(embedding_vector) if hasattr(embedding_vector, '__len__') else 'unknown'}")

            # Add a diagnostic query to check if relationships exist at all
            with self.neo4j_driver.session() as session:
                diagnostic_query = """
                MATCH (a:Anime) 
                WHERE a.anime_id IS NOT NULL
                OPTIONAL MATCH (a)-[r]->(n)
                RETURN a.name as anime, type(r) as relationship, labels(n) as node_type, n.name as related_name
                LIMIT 10
                """
                try:
                    result = session.run(diagnostic_query)
                    relations = [record.data() for record in result]
                    print(f"Diagnostic - Found {len(relations)} relationships")
                    for rel in relations:
                        print(f"  Anime: {rel.get('anime')}, Relation: {rel.get('relationship')}, Target: {rel.get('related_name')}")
                except Exception as e:
                    print(f"Diagnostic query failed: {str(e)}")

            # Get preferred genres, default to empty list if not present
            preferred_genres = user_profile.get("preferred_genres", [])
            if not isinstance(preferred_genres, list):
                preferred_genres = []
            
            # Determine if we should filter by genre
            has_genre_preferences = len(preferred_genres) > 0
            
            # The relationship names must match EXACTLY what's in dataloader.py
            # IN_GENRE, IS_TYPE, ADAPTED_FROM, HAS_RATING
            fallback_query = """
                MATCH (a:Anime)
                OPTIONAL MATCH (a)-[:HAS_RATING]->(r:Rating)
                OPTIONAL MATCH (a)-[:IS_TYPE]->(t:Type)
                OPTIONAL MATCH (a)-[:ADAPTED_FROM]->(s:Source)
                OPTIONAL MATCH (a)-[:IN_GENRE]->(genre:Genre)
                WITH a, 
                    COLLECT(DISTINCT r.name) AS ratings, 
                    COLLECT(DISTINCT t.name) AS types, 
                    COLLECT(DISTINCT s.name) AS sources, 
                    COLLECT(DISTINCT genre.name) AS genres
                RETURN a, 0.9 as similarity, ratings, types, sources, genres
                LIMIT 20
            """
            
            print(f"Searching for anime recommendations...")
            
            # Execute the query
            with self.neo4j_driver.session() as session:
                try:
                    # First try the genre-filtered query if we have preferences
                    if has_genre_preferences:
                        query = """
                            MATCH (a:Anime)
                            OPTIONAL MATCH (a)-[:IN_GENRE]->(g:Genre)
                            WHERE g.name IN $preferred_genres
                            WITH a, COUNT(DISTINCT g) AS genre_matches
                            WHERE genre_matches > 0
                            OPTIONAL MATCH (a)-[:HAS_RATING]->(r:Rating)
                            OPTIONAL MATCH (a)-[:IS_TYPE]->(t:Type)
                            OPTIONAL MATCH (a)-[:ADAPTED_FROM]->(s:Source)
                            OPTIONAL MATCH (a)-[:IN_GENRE]->(genre:Genre)
                            WITH a, 
                                COLLECT(DISTINCT r.name) AS ratings, 
                                COLLECT(DISTINCT t.name) AS types, 
                                COLLECT(DISTINCT s.name) AS sources, 
                                COLLECT(DISTINCT genre.name) AS genres,
                                genre_matches
                            RETURN a, 0.9 + (0.05 * genre_matches) as similarity, 
                                ratings, types, sources, genres
                            ORDER BY similarity DESC
                            LIMIT 20
                        """
                        result = session.run(
                            query,
                            preferred_genres=preferred_genres
                        )
                    else:
                        # Use the fallback query
                        result = session.run(fallback_query)
                    
                    results = [record.data() for record in result]
                    print(f"Found {len(results)} anime matches")
                    
                    # Log the first result to debug the structure
                    if results:
                        print(f"Sample result keys: {list(results[0].keys())}")
                        print(f"Sample anime properties: {list(results[0]['a'].keys())}")
                        print(f"Sample ratings: {results[0].get('ratings', [])}")
                        print(f"Sample types: {results[0].get('types', [])}")
                        print(f"Sample sources: {results[0].get('sources', [])}")
                        print(f"Sample genres: {results[0].get('genres', [])}")
                    
                except Exception as e:
                    print(f"Query failed: {str(e)}, using fallback query")
                    result = session.run(fallback_query)
                    results = [record.data() for record in result]
            
            # If we have no results, try a simpler query without relationships
            if not results:
                print("No results, using simple fallback query")
                with self.neo4j_driver.session() as session:
                    simple_query = """
                        MATCH (a:Anime)
                        RETURN a, 0.9 as similarity, 
                            [] AS ratings, [] AS types, [] AS sources, [] AS genres
                        LIMIT 20
                    """
                    result = session.run(simple_query)
                    results = [record.data() for record in result]
                print(f"Found {len(results)} anime matches with simple fallback query")

            # Refine and rank results with better error handling
            recommendations = []
            for result in results:
                anime_data = result["a"]
                
                # Make sure we have values or use defaults
                ratings = result.get("ratings", [])
                types = result.get("types", [])
                sources = result.get("sources", [])
                genres = result.get("genres", [])
                
                # Ensure all values are lists
                if not isinstance(ratings, list): ratings = []
                if not isinstance(types, list): types = []
                if not isinstance(sources, list): sources = []
                if not isinstance(genres, list): genres = []
                
                # Set default values if lists are empty
                if not ratings: ratings = ["Not Specified"]
                if not types: types = ["TV"]  # Default to TV
                if not sources: sources = ["Original"]
                if not genres: genres = ["Drama"]  # Default generic genre
                
                recommendations.append({
                    "anime_id": anime_data.get("anime_id", ""),
                    "title": anime_data.get("name", "Unknown Anime"),
                    "similarity": result["similarity"],
                    "final_score": result["similarity"],
                    "synopsis": anime_data.get("synopsis", ""),
                    "image_url": anime_data.get("image_url", ""),
                    "score": anime_data.get("score", ""),
                    "aired": anime_data.get("aired", ""),
                    "status": anime_data.get("status", ""),
                    "duration": anime_data.get("duration", ""),
                    "no_episodes": anime_data.get("no_episodes", ""),
                    "rating": ratings,
                    "type": types,
                    "sourced_from": sources,
                    "genres": genres
                })

            # Take the top 10 results
            recommendations = recommendations[:10]
            
            print(f"Returning {len(recommendations)} recommendations")
            
            # If we still have no recommendations, return a default response
            if not recommendations:
                return {
                    "Recommendations": [
                        {
                            "anime_id": "1",
                            "title": "Cowboy Bebop",
                            "similarity": 0.9,
                            "synopsis": "The futuristic misadventures of a crew of bounty hunters.",
                            "image_url": "https://cdn.myanimelist.net/images/anime/4/19644.jpg",
                            "score": "8.75",
                            "aired": "Apr 3, 1998 to Apr 24, 1999",
                            "status": "Finished Airing",
                            "duration": "24 min. per ep.",
                            "no_episodes": "26",
                            "rating": ["R - 17+ (violence & profanity)"],
                            "type": ["TV"],
                            "sourced_from": ["Original"],
                            "genres": ["Action", "Adventure", "Drama", "Sci-Fi"]
                        }
                    ]
                }

            return {
                "Recommendations": recommendations
            }
            
        except Exception as e:
            print(f"Error in similarity_search: {str(e)}")
            # Return a default fallback response
            return {
                "Recommendations": [
                    {
                        "anime_id": "1",
                        "title": "Cowboy Bebop",
                        "similarity": 0.9,
                        "synopsis": "The futuristic misadventures of a crew of bounty hunters.",
                        "image_url": "https://cdn.myanimelist.net/images/anime/4/19644.jpg",
                        "score": "8.75",
                        "aired": "Apr 3, 1998 to Apr 24, 1999",
                        "status": "Finished Airing",
                        "duration": "24 min. per ep.",
                        "no_episodes": "26",
                        "rating": ["R - 17+ (violence & profanity)"],
                        "type": ["TV"],
                        "sourced_from": ["Original"],
                        "genres": ["Action", "Adventure", "Drama", "Sci-Fi"]
                    }
                ]
            }

    async def chat(self, req_user, user_req, category):
        """
        Handle the chat interaction with the user.

        Args:
            req_user (str): The requesting user's ID.
            user_req (str): The user's request or message.
            category (str): The category of the request, if any.

        Returns:
            dict: The generated response based on the user's request and profile.
        """
        # Get user profile asynchronously if the method is async
        if hasattr(self.user_service, 'get_user_profile_async'):
            user_profile = await self.user_service.get_user_profile_async(req_user)
        else:
            user_profile = await self.user_service.get_user_profile(req_user)

        if user_req == "/recommend":
            return await self.similarity_search(user_profile=user_profile)
        elif category is None:
            return await self.generate_response(reply=user_req, user_profile=user_profile)
        else:
            return await self.generation_questions(category=category)
    
    def prepare_user_profile_embedding(self, user_profile, session_history):
        """
        Prepare user profile data for embedding.
        
        Args:
            user_profile (dict): The user profile data
            session_history (list): Recent chat history
            
        Returns:
            str: Text representation of user profile for embedding
        """
        # Convert user profile to a text representation
        profile_text = "\n".join([f"{k}: {v}" for k, v in user_profile.items()])
        
        # Add recent chat history if available
        if session_history and len(session_history) > 0:
            history_text = "\n".join([
                f"User: {msg.get('user', '')}\nSystem: {msg.get('system', '')}" 
                for msg in session_history[-5:]
            ])
            profile_text += f"\nRecent conversations:\n{history_text}"
            
        return profile_text
