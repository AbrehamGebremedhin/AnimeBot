from fastapi import APIRouter, Depends, BackgroundTasks, HTTPException
from typing import Dict, Any
import logging

from app.utils.neo4j_connection import Neo4jConnection
from app.utils.diversity_service import DiversityService

maintenance_router = APIRouter(
    prefix="/maintenance",
    tags=["maintenance"],
)

logger = logging.getLogger(__name__)

def get_diversity_service():
    neo4j_connection = Neo4jConnection()
    return DiversityService(neo4j_connection)

@maintenance_router.post("/refresh-recommendations")
async def refresh_recommendations(
    background_tasks: BackgroundTasks,
    diversity_service: DiversityService = Depends(get_diversity_service)
):
    """
    Refresh recommendation weights to ensure diversity in anime recommendations
    """
    background_tasks.add_task(diversity_service.refresh_recommendation_weights)
    background_tasks.add_task(diversity_service.clean_expired_recommendations)
    
    return {"message": "Recommendation refresh scheduled in background"}

@maintenance_router.get("/genre-distribution")
async def genre_distribution(
    diversity_service: DiversityService = Depends(get_diversity_service)
):
    """
    Get the distribution of genres in the anime database
    """
    distribution = await diversity_service.analyze_genre_distribution()
    
    if not distribution:
        raise HTTPException(status_code=500, detail="Failed to analyze genre distribution")
    
    return {"genre_distribution": distribution}

@maintenance_router.post("/clear-user-history/{user_id}")
async def clear_user_history(
    user_id: str,
    diversity_service: DiversityService = Depends(get_diversity_service)
):
    """
    Clear a user's recommendation and chat history to reset recommendations
    """
    try:
        async with diversity_service.neo4j_connection.get_async_driver().session() as session:
            query = """
            MATCH (u:User {user_id: $user_id})-[r]->(a:Anime)
            WHERE type(r) IN ['RECEIVED_RECOMMENDATION', 'MENTIONED_IN_CHAT']
            DELETE r
            RETURN count(r) as deleted_count
            """
            result = await session.run(query, {"user_id": user_id})
            record = await result.single()
            deleted_count = record["deleted_count"] if record else 0
            
            logger.info(f"Cleared {deleted_count} history relationships for user {user_id}")
            return {"message": f"Successfully cleared {deleted_count} history entries"}
    except Exception as e:
        logger.error(f"Failed to clear history for user {user_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to clear history: {str(e)}")

@maintenance_router.post("/boost-diversity")
async def boost_diversity(
    background_tasks: BackgroundTasks,
    diversity_service: DiversityService = Depends(get_diversity_service)
):
    """
    Boost diversity by increasing weights for less frequently recommended anime
    """
    background_tasks.add_task(diversity_service.boost_diversity_weights)
    
    return {"message": "Diversity boost scheduled in background"}
