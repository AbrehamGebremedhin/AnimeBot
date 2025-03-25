import logging
import asyncio
import random
from typing import List, Dict, Any
import time
from app.utils.neo4j_connection import Neo4jConnection

class DiversityService:
    """
    Service to ensure diversity in anime recommendations
    """
    
    def __init__(self, neo4j_connection: Neo4jConnection):
        self.neo4j_connection = neo4j_connection
        self.logger = logging.getLogger(__name__)
    
    async def refresh_recommendation_weights(self):
        """
        Refresh randomness weights for anime to ensure diversity in recommendations
        """
        try:
            async with self.neo4j_connection.get_async_driver().session() as session:
                query = """
                MATCH (a:Anime)
                SET a.diversity_weight = rand()
                RETURN count(a) as updated_count
                """
                result = await session.run(query)
                record = await result.single()
                updated_count = record["updated_count"] if record else 0
                
                self.logger.info(f"Updated diversity weights for {updated_count} anime")
                return updated_count
        except Exception as e:
            self.logger.error(f"Error refreshing recommendation weights: {str(e)}")
            return 0
    
    async def analyze_genre_distribution(self):
        """
        Analyze the distribution of genres in the database
        """
        try:
            async with self.neo4j_connection.get_async_driver().session() as session:
                query = """
                MATCH (g:Genre)<-[:IN_GENRE]-(a:Anime)
                RETURN g.name as genre, count(a) as anime_count
                ORDER BY anime_count DESC
                """
                result = await session.run(query)
                records = await result.to_list()
                
                genre_distribution = {record["genre"]: record["anime_count"] for record in records}
                total_anime = sum(genre_distribution.values())
                
                if total_anime > 0:
                    genre_percentages = {genre: (count / total_anime) * 100 
                                        for genre, count in genre_distribution.items()}
                    
                    self.logger.info(f"Genre distribution analysis complete. Top genres: "
                                    f"{sorted(genre_percentages.items(), key=lambda x: x[1], reverse=True)[:5]}")
                    return genre_percentages
                return {}
        except Exception as e:
            self.logger.error(f"Error analyzing genre distribution: {str(e)}")
            return {}
    
    async def clean_expired_recommendations(self, days=14):
        """
        Remove recommendation relationships older than specified days
        to allow new recommendations to be made
        """
        return await self.neo4j_connection.clean_old_recommendations(days)
    
    async def get_least_recommended_anime(self, limit=30, excluded_ids=None):
        """
        Get anime that have been recommended the least number of times
        
        Args:
            limit (int): Maximum number of anime to return
            excluded_ids (list): List of anime IDs to exclude
            
        Returns:
            list: List of anime IDs that are least frequently recommended
        """
        try:
            if excluded_ids is None:
                excluded_ids = []
                
            async with self.neo4j_connection.get_async_driver().session() as session:
                query = """
                MATCH (a:Anime)
                WHERE NOT a.anime_id IN $excluded_ids
                OPTIONAL MATCH (u:User)-[r:RECEIVED_RECOMMENDATION]->(a)
                WITH a, COUNT(r) as recommendation_count
                RETURN a.anime_id as anime_id, a.name as name, recommendation_count
                ORDER BY recommendation_count ASC, rand() DESC
                LIMIT $limit
                """
                
                result = await session.run(query, {
                    "excluded_ids": excluded_ids,
                    "limit": limit
                })
                
                records = await result.to_list()
                anime_ids = [record["anime_id"] for record in records]
                
                self.logger.info(f"Found {len(anime_ids)} least recommended anime")
                return anime_ids
                
        except Exception as e:
            self.logger.error(f"Error getting least recommended anime: {str(e)}")
            return []
            
    async def boost_diversity_weights(self):
        """
        Boost diversity weights for anime that haven't been recommended frequently
        """
        try:
            async with self.neo4j_connection.get_async_driver().session() as session:
                query = """
                MATCH (a:Anime)
                OPTIONAL MATCH (u:User)-[r:RECEIVED_RECOMMENDATION]->(a)
                WITH a, COUNT(r) as rec_count
                SET a.diversity_weight = 1.0 - (rec_count * 0.01)
                RETURN count(a) as updated_count
                """
                
                result = await session.run(query)
                record = await result.single()
                updated_count = record["updated_count"] if record else 0
                
                self.logger.info(f"Boosted diversity weights for {updated_count} anime based on recommendation frequency")
                return updated_count
                
        except Exception as e:
            self.logger.error(f"Error boosting diversity weights: {str(e)}")
            return 0
