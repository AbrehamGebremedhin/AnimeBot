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
        
        # Add in-memory cache of recent recommendations to prevent repeats in sequential requests
        self.recent_recommendations = []
    
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
            # Get previously recommended anime IDs for this user
            previously_recommended = []
            if 'user_id' in user_profile:
                previously_recommended = await self.get_previously_recommended(user_profile['user_id'])
                
            # Also exclude recently recommended anime from this session (in-memory cache)
            # This prevents repeats even if DB writes haven't completed yet
            previously_recommended.extend(self.recent_recommendations)
            previously_recommended = list(set(previously_recommended))  # Remove duplicates
            
            print(f"Excluding {len(previously_recommended)} previously recommended anime")
            
            # Extract anime and genre mentions from chat history
            chat_mentions = self.extract_chat_mentions(self.session_history)
            mentioned_genres = chat_mentions.get('genres', [])
            mentioned_anime = chat_mentions.get('anime_titles', [])
            
            # Prepare the user profile for embedding with enhanced chat history analysis
            user_profile_text = self.prepare_user_profile_embedding(user_profile, self.session_history, chat_mentions)
            
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
            
            # Extract genre preferences from user profile
            preferred_genres = []
            has_genre_preferences = False
            
            if user_profile and "preferences" in user_profile:
                genre_question = "What genres do you enjoy?"
                for item in user_profile["preferences"]:
                    if item.get("question") == genre_question and item.get("answer"):
                        genre_answers = item.get("answer", "")
                        # Split by commas, clean up, and filter empty strings
                        preferred_genres = [g.strip() for g in genre_answers.split(",") if g.strip()]
                        has_genre_preferences = len(preferred_genres) > 0
                        break
            
            # Combine profile genres with genres mentioned in chat
            if mentioned_genres:
                preferred_genres = list(set(preferred_genres + mentioned_genres))
                has_genre_preferences = True
                print(f"Combined genres from profile and chat: {preferred_genres}")
            
            # If no specific genres provided, get diverse genres for variety
            if not has_genre_preferences:
                preferred_genres = ["Action", "Comedy", "Drama", "Romance", "Fantasy", "Sci-Fi", "Slice of Life", "Mystery"]
                has_genre_preferences = True
            
            # Enhanced query that considers chat history mentions and uses stronger randomization
            query = """
                MATCH (a:Anime)
                WHERE NOT a.anime_id IN $previously_recommended
                
                // Score based on genre matches from profile and chat
                OPTIONAL MATCH (a)-[:IN_GENRE]->(g:Genre)
                WHERE g.name IN $preferred_genres
                WITH a, COUNT(DISTINCT g) AS genre_matches
                
                // Boost score for anime mentioned in chat
                OPTIONAL MATCH (a)
                WHERE a.name IN $mentioned_anime
                WITH a, genre_matches, 
                     CASE WHEN a.name IN $mentioned_anime THEN 0.3 ELSE 0 END AS mention_boost
                
                // Get related information
                OPTIONAL MATCH (a)-[:HAS_RATING]->(r:Rating)
                OPTIONAL MATCH (a)-[:IS_TYPE]->(t:Type)
                OPTIONAL MATCH (a)-[:ADAPTED_FROM]->(s:Source)
                OPTIONAL MATCH (a)-[:IN_GENRE]->(genre:Genre)
                
                // Enhanced randomness with timestamp-based seed
                WITH a, genre_matches, mention_boost,
                    COLLECT(DISTINCT r.name) AS ratings, 
                    COLLECT(DISTINCT t.name) AS types, 
                    COLLECT(DISTINCT s.name) AS sources, 
                    COLLECT(DISTINCT genre.name) AS genres,
                    RAND() * 0.3 AS diversity_factor,
                    // Ensure different results even with same query parameters
                    (timestamp() % 1000) / 1000.0 * 0.1 AS time_factor
                    
                RETURN a, 
                    CASE 
                        WHEN genre_matches > 0 THEN 0.6 + (genre_matches * 0.05) + mention_boost + diversity_factor + time_factor
                        ELSE 0.4 + mention_boost + diversity_factor + time_factor
                    END as similarity, 
                    ratings, types, sources, genres
                ORDER BY similarity DESC, diversity_factor DESC, time_factor DESC
                LIMIT 50
            """
            
            with self.neo4j_driver.session() as session:
                try:
                    # Use our improved query that includes chat history
                    result = session.run(query, {
                        "preferred_genres": preferred_genres,
                        "mentioned_anime": mentioned_anime,
                        "previously_recommended": previously_recommended
                    })
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
                    # Try a more diverse fallback query that still considers chat mentions
                    fallback_query = """
                        MATCH (a:Anime)
                        WHERE NOT a.anime_id IN $previously_recommended
                        
                        WITH a, 
                             CASE WHEN a.name IN $mentioned_anime THEN 0.3 ELSE 0 END AS mention_boost,
                             RAND() * 0.3 AS diversity_factor
                             
                        OPTIONAL MATCH (a)-[:HAS_RATING]->(r:Rating)
                        OPTIONAL MATCH (a)-[:IS_TYPE]->(t:Type)
                        OPTIONAL MATCH (a)-[:ADAPTED_FROM]->(s:Source)
                        OPTIONAL MATCH (a)-[:IN_GENRE]->(genre:Genre)
                        
                        WITH a, 
                            COLLECT(DISTINCT r.name) AS ratings, 
                            COLLECT(DISTINCT t.name) AS types, 
                            COLLECT(DISTINCT s.name) AS sources, 
                            COLLECT(DISTINCT genre.name) AS genres,
                            mention_boost,
                            diversity_factor
                            
                        RETURN a, 0.5 + mention_boost + diversity_factor as similarity, 
                               ratings, types, sources, genres
                        ORDER BY similarity DESC
                        LIMIT 30
                    """
                    result = session.run(fallback_query, {
                        "mentioned_anime": mentioned_anime,
                        "previously_recommended": previously_recommended
                    })
                    results = [record.data() for record in result]
            
            # If we have no results, try a simpler query without relationships
            if not results:
                print("No results, using simple fallback query")
                with self.neo4j_driver.session() as session:
                    simple_query = """
                        MATCH (a:Anime)
                        WHERE NOT a.anime_id IN $previously_recommended
                        WITH a, RAND() AS random_factor
                        RETURN a, 0.6 + (random_factor * 0.3) as similarity, 
                            [] AS ratings, [] AS types, [] AS sources, [] AS genres
                        ORDER BY random_factor DESC
                        LIMIT 30
                    """
                    result = session.run(simple_query, {
                        "previously_recommended": previously_recommended
                    })
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

            # Apply additional diversity - ensure mix of genres in top results
            recommendations = self.diversify_recommendations(recommendations)
                
            # Take more than we need to have extras if needed
            top_recommendations = recommendations[:15]
            
            # Now apply additional filtering to avoid repeating recent recommendations
            final_recommendations = []
            anime_count = 0
            
            # First pass: Add recommendations with unique genres compared to recent recs
            recent_genres = self.get_recent_recommendation_genres()
            for rec in top_recommendations:
                # Skip if we already have enough 
                if anime_count >= 10:
                    break
                    
                # Check if this anime has at least one genre that hasn't been in recent recommendations
                rec_genres = set(rec.get("genres", []))
                if any(genre not in recent_genres for genre in rec_genres):
                    final_recommendations.append(rec)
                    anime_count += 1
                    # Add these genres to recent genres to ensure diversity in this batch too
                    recent_genres.update(rec_genres)
            
            # Second pass: If we still need more, add any remaining recommendations
            if anime_count < 10:
                for rec in top_recommendations:
                    if rec not in final_recommendations and anime_count < 10:
                        final_recommendations.append(rec)
                        anime_count += 1
            
            print(f"Final recommendations count: {len(final_recommendations)}")
            
            # Update in-memory cache with new recommendations to prevent immediate repeats
            # Only store anime_ids to keep memory usage low
            self.update_recent_recommendations([rec["anime_id"] for rec in final_recommendations])
            
            # Store recommendations for this user if user_id is available - do this BEFORE returning
            # to ensure relationships are created before next request
            if 'user_id' in user_profile and final_recommendations:
                await self.store_recommendations_sync(user_profile['user_id'], 
                                                    [rec["anime_id"] for rec in final_recommendations])
                
                # Also store anime mentioned in chat for future reference
                if mentioned_anime:
                    await self.store_chat_mentions(user_profile['user_id'], mentioned_anime)
            
            # If we still have no recommendations, return a default response with variety
            if not final_recommendations:
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
                        },
                        {
                            "anime_id": "5114",
                            "title": "Fullmetal Alchemist: Brotherhood",
                            "similarity": 0.89,
                            "synopsis": "Two brothers search for a Philosopher's Stone after an attempt to revive their deceased mother goes wrong.",
                            "image_url": "https://cdn.myanimelist.net/images/anime/1223/96541.jpg",
                            "score": "9.10",
                            "aired": "Apr 5, 2009 to Jul 4, 2010",
                            "status": "Finished Airing",
                            "duration": "24 min. per ep.",
                            "no_episodes": "64",
                            "rating": ["R - 17+ (violence & profanity)"],
                            "type": ["TV"],
                            "sourced_from": ["Manga"],
                            "genres": ["Action", "Adventure", "Drama", "Fantasy"]
                        },
                        {
                            "anime_id": "9253",
                            "title": "Steins;Gate",
                            "similarity": 0.87,
                            "synopsis": "A group of friends create a device that can send messages to the past, with unforeseen consequences.",
                            "image_url": "https://cdn.myanimelist.net/images/anime/1935/127974.jpg",
                            "score": "9.08",
                            "aired": "Apr 6, 2011 to Sep 14, 2011",
                            "status": "Finished Airing",
                            "duration": "24 min. per ep.",
                            "no_episodes": "24",
                            "rating": ["PG-13 - Teens 13 or older"],
                            "type": ["TV"],
                            "sourced_from": ["Visual Novel"],
                            "genres": ["Drama", "Sci-Fi", "Suspense", "Psychological"]
                        }
                    ]
                }

            return {
                "Recommendations": final_recommendations
            }
            
        except Exception as e:
            print(f"Error in similarity_search: {str(e)}")
            # Return a default fallback response with diversity
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
                    },
                    {
                        "anime_id": "30276",
                        "title": "One Punch Man",
                        "similarity": 0.88,
                        "synopsis": "The story of Saitama, a hero who can defeat any opponent with a single punch but seeks a worthy opponent after growing bored by a lack of challenge.",
                        "image_url": "https://cdn.myanimelist.net/images/anime/12/76049.jpg",
                        "score": "8.50",
                        "aired": "Oct 5, 2015 to Dec 21, 2015",
                        "status": "Finished Airing",
                        "duration": "24 min. per ep.",
                        "no_episodes": "12",
                        "rating": ["R - 17+ (violence & profanity)"],
                        "type": ["TV"],
                        "sourced_from": ["Web Manga"],
                        "genres": ["Action", "Comedy", "Sci-Fi", "Supernatural"]
                    }
                ]
            }

    async def get_previously_recommended(self, user_id):
        """Get previously recommended anime IDs for a user"""
        try:
            with self.neo4j_driver.session() as session:
                # Modified query to get only anime recommended in the last 30 days
                # This prevents the list from growing too large over time
                query = """
                MATCH (u:User {user_id: $user_id})-[r:RECEIVED_RECOMMENDATION]->(a:Anime)
                WHERE r.timestamp > (timestamp() - (30 * 24 * 60 * 60 * 1000)) 
                RETURN a.anime_id as anime_id
                UNION
                MATCH (u:User {user_id: $user_id})-[:MENTIONED_IN_CHAT {sentiment: 'negative'}]->(a:Anime)
                RETURN a.anime_id as anime_id
                """
                result = session.run(query, {"user_id": user_id})
                return [record["anime_id"] for record in result]
        except Exception as e:
            print(f"Error getting previous recommendations: {str(e)}")
            return []

    async def store_recommendations(self, user_id, anime_ids):
        """Store recommendations for a user"""
        try:
            with self.neo4j_driver.session() as session:
                # Create relationships between user and recommended anime
                for anime_id in anime_ids:
                    query = """
                    MERGE (u:User {user_id: $user_id})
                    MERGE (a:Anime {anime_id: $anime_id})
                    MERGE (u)-[r:RECEIVED_RECOMMENDATION]->(a)
                    SET r.timestamp = timestamp()
                    """
                    session.run(query, {"user_id": user_id, "anime_id": str(anime_id)})
            print(f"Stored {len(anime_ids)} recommendations for user {user_id}")
        except Exception as e:
            print(f"Error storing recommendations: {str(e)}")

    def diversify_recommendations(self, recommendations):
        """Ensure diversity in recommendations by prioritizing different genres"""
        if not recommendations or len(recommendations) < 3:
            return recommendations
            
        # Helper function to calculate genre diversity score with stronger weighting
        def genre_diversity_score(rec, selected_genres):
            rec_genres = rec.get("genres", [])
            # Higher score for anime with genres we haven't recommended yet
            new_genres = sum(2 for g in rec_genres if g not in selected_genres)
            # Penalty for having too many already-seen genres
            overlap = sum(1 for g in rec_genres if g in selected_genres)
            # Final score balances new genres against overlap
            return new_genres - (0.5 * overlap)
        
        diversified = []
        selected_genres = set()
        
        # First, take the top scoring anime
        diversified.append(recommendations[0])
        for genre in recommendations[0].get("genres", []):
            selected_genres.add(genre)
        
        # Create a copy of remaining recommendations to work with
        remaining = recommendations[1:].copy()
        
        # Add the rest with diversity consideration
        while remaining and len(diversified) < 10:
            # Calculate diversity scores
            for rec in remaining:
                rec["diversity_score"] = genre_diversity_score(rec, selected_genres)
            
            # Sort by a combination of similarity and diversity with stronger diversity weight
            remaining.sort(key=lambda x: (x["similarity"] * 0.6) + (x["diversity_score"] * 0.4), reverse=True)
            
            # Take the top after resorting
            next_rec = remaining.pop(0)
            
            # Skip if diversity score is too low (indicating too much genre overlap)
            if next_rec["diversity_score"] <= 0 and len(diversified) >= 5:
                continue
                
            diversified.append(next_rec)
            
            # Update selected genres
            for genre in next_rec.get("genres", []):
                selected_genres.add(genre)
        
        return diversified

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
    
    def prepare_user_profile_embedding(self, user_profile, session_history, chat_mentions=None):
        """
        Prepare user profile data for embedding with enhanced chat analysis.
        
        Args:
            user_profile (dict): The user profile data
            session_history (list): Recent chat history
            chat_mentions (dict): Extracted mentions from chat history
            
        Returns:
            str: Text representation of user profile for embedding
        """
        # Convert user profile to a text representation
        profile_parts = []
        
        # Add core profile data
        if user_profile:
            for category, items in user_profile.items():
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, dict) and "question" in item and "answer" in item:
                            profile_parts.append(f"{item['question']}: {item['answer']}")
        
        # Add extracted information from chat history if available
        if chat_mentions:
            if chat_mentions.get('genres'):
                profile_parts.append(f"Genres mentioned in conversation: {', '.join(chat_mentions['genres'])}")
            
            if chat_mentions.get('anime_titles'):
                profile_parts.append(f"Anime discussed in conversation: {', '.join(chat_mentions['anime_titles'])}")
                
                # Add sentiment information if available
                for anime, sentiment in chat_mentions.get('sentiment', {}).items():
                    if sentiment in ["positive", "negative"]:
                        profile_parts.append(f"User {sentiment}ly mentioned anime: {anime}")
        
        # Add recent chat history if available
        if session_history and len(session_history) > 0:
            history_text = []
            for msg in session_history[-5:]:
                if 'user' in msg and 'system' in msg:
                    history_text.append(f"User: {msg['user']}")
                    history_text.append(f"System: {msg['system']}")
            
            if history_text:
                profile_parts.append("Recent conversations:")
                profile_parts.extend(history_text)
        
        # Join all parts with newlines
        return "\n".join(profile_parts)

    def extract_chat_mentions(self, session_history):
        """
        Extract mentions of anime titles and genres from chat history
        
        Args:
            session_history (list): List of chat history messages
        
        Returns:
            dict: Dictionary with extracted anime titles and genres
        """
        result = {
            'anime_titles': [],
            'genres': [],
            'sentiment': {}  # Tracks positive/negative mentions
        }
        
        # Common anime genres to look for in chat
        common_genres = [
            "Action", "Adventure", "Comedy", "Drama", "Fantasy", "Horror", "Mystery", 
            "Romance", "Sci-Fi", "Slice of Life", "Sports", "Supernatural", "Thriller",
            "Mecha", "Psychological", "Isekai", "Shounen", "Shoujo", "Seinen", "Josei"
        ]
        
        # Only process if we have history
        if not session_history:
            return result
        
        # Analyze the most recent 10 messages (or fewer if history is shorter)
        recent_messages = session_history[-10:]
        
        for msg in recent_messages:
            if 'user' not in msg:
                continue
                
            user_text = msg['user'].lower()
            
            # Check for phrases indicating anime mentions 
            # (can be expanded for more sophisticated detection)
            likes_indicators = ["like", "love", "enjoy", "favorite", "great", "awesome", "amazing"]
            dislikes_indicators = ["dislike", "hate", "boring", "terrible", "awful", "not good"]
            
            # Simple sentiment analysis
            positive = any(indicator in user_text for indicator in likes_indicators)
            negative = any(indicator in user_text for indicator in dislikes_indicators)
            sentiment = "positive" if positive and not negative else "negative" if negative else "neutral"
            
            # Extract potential anime titles (basic approach - could be enhanced with NER)
            if "anime" in user_text and any(indicator in user_text for indicator in likes_indicators + dislikes_indicators):
                # Basic extraction of potential anime titles
                # This is a simplistic approach that could be enhanced with better NLP techniques
                words = user_text.split()
                for i, word in enumerate(words):
                    if word == "anime" and i > 0:
                        potential_title = words[i-1]
                        if len(potential_title) > 3 and potential_title not in result['anime_titles']:
                            result['anime_titles'].append(potential_title)
                            result['sentiment'][potential_title] = sentiment
            
            # Look for genre mentions
            for genre in common_genres:
                if genre.lower() in user_text:
                    if genre not in result['genres']:
                        result['genres'].append(genre)
        
        print(f"Extracted from chat: {result}")
        return result

    async def store_chat_mentions(self, user_id, anime_titles):
        """Store anime titles mentioned in chat for a user"""
        try:
            with self.neo4j_driver.session() as session:
                for title in anime_titles:
                    query = """
                    MERGE (u:User {user_id: $user_id})
                    MATCH (a:Anime)
                    WHERE toLower(a.name) CONTAINS toLower($title)
                    WITH u, a LIMIT 1
                    MERGE (u)-[r:MENTIONED_IN_CHAT]->(a)
                    SET r.timestamp = timestamp()
                    """
                    session.run(query, {"user_id": user_id, "title": title})
            print(f"Stored {len(anime_titles)} chat mentions for user {user_id}")
        except Exception as e:
            print(f"Error storing chat mentions: {str(e)}")

    def update_recent_recommendations(self, anime_ids):
        """
        Update the in-memory cache of recent recommendations
        
        Args:
            anime_ids (list): List of anime IDs to add to recent recommendations
        """
        # Add new recommendations to the front of the list
        self.recent_recommendations = anime_ids + self.recent_recommendations
        # Keep only the most recent 40 recommendations (prevent memory bloat)
        self.recent_recommendations = self.recent_recommendations[:40]
        print(f"Updated recent recommendations cache. Now tracking {len(self.recent_recommendations)} recent anime.")
        
    def get_recent_recommendation_genres(self):
        """
        Get a set of genres from recently recommended anime
        
        Returns:
            set: Set of recently recommended genres
        """
        # Get genres from our Neo4j database for recently recommended anime
        genres = set()
        try:
            with self.neo4j_driver.session() as session:
                # Get genres for our recently recommended anime
                if self.recent_recommendations:
                    query = """
                    MATCH (a:Anime)
                    WHERE a.anime_id IN $anime_ids
                    RETURN a.genres as genres
                    """
                    result = session.run(query, {"anime_ids": self.recent_recommendations[:20]})
                    for record in result:
                        if record["genres"]:
                            # Add all genres to our set
                            anime_genres = record["genres"]
                            if isinstance(anime_genres, list):
                                genres.update(anime_genres)
        except Exception as e:
            print(f"Error getting recent recommendation genres: {str(e)}")
        
        return genres

    async def store_recommendations_sync(self, user_id, anime_ids):
        """Store recommendations for a user synchronously (wait for completion)"""
        try:
            with self.neo4j_driver.session() as session:
                # Use a transaction to ensure all writes complete
                with session.begin_transaction() as tx:
                    for anime_id in anime_ids:
                        query = """
                        MERGE (u:User {user_id: $user_id})
                        MERGE (a:Anime {anime_id: $anime_id})
                        MERGE (u)-[r:RECEIVED_RECOMMENDATION]->(a)
                        SET r.timestamp = timestamp()
                        """
                        tx.run(query, {"user_id": user_id, "anime_id": str(anime_id)})
                    
                    # Commit the transaction explicitly
                    tx.commit()
                    
            print(f"Stored {len(anime_ids)} recommendations for user {user_id} synchronously")
        except Exception as e:
            print(f"Error storing recommendations synchronously: {str(e)}")
