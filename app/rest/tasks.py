from fastapi import APIRouter, HTTPException, status, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import uuid
from ..utils.background_tasks import task_manager

tasks_router = APIRouter(prefix="/tasks", tags=["tasks"])

class TaskRequest(BaseModel):
    """Request model for creating a task"""
    task_type: str
    parameters: Dict[str, Any]
    timeout: Optional[float] = None

class TaskResponse(BaseModel):
    """Response model for task operations"""
    task_id: str
    status: str

@tasks_router.post("/", response_model=TaskResponse)
async def create_task(request: TaskRequest, background_tasks: BackgroundTasks):
    """
    Create a new background task
    
    Example request body:
    ```json
    {
        "task_type": "recommendation_refresh",
        "parameters": {
            "user_id": 123
        },
        "timeout": 300
    }
    ```
    """
    task_id = str(uuid.uuid4())
    
    # Map task type to actual coroutine
    if request.task_type == "recommendation_refresh":
        # This would call a function that refreshes recommendations
        # For now, we'll just simulate with asyncio.sleep
        import asyncio
        
        async def refresh_task():
            await asyncio.sleep(5)  # Simulate work
            return {"message": "Recommendations refreshed successfully"}
        
        coroutine = refresh_task()
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown task type: {request.task_type}"
        )
    
    # Add task to manager
    await task_manager.add_task(
        task_id=task_id,
        coroutine=coroutine,
        timeout=request.timeout
    )
    
    # Also schedule cleanup to run after a day
    background_tasks.add_task(task_manager.cleanup, 86400)
    
    return TaskResponse(
        task_id=task_id,
        status="scheduled"
    )

@tasks_router.get("/{task_id}", response_model=Dict[str, Any])
async def get_task_status(task_id: str):
    """Get the status of a specific task"""
    status = task_manager.get_task_status(task_id)
    if status.get("status") == "not_found":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task with ID {task_id} not found"
        )
    return status

@tasks_router.delete("/{task_id}", response_model=TaskResponse)
async def cancel_task(task_id: str):
    """Cancel a running task"""
    cancelled = task_manager.cancel_task(task_id)
    if not cancelled:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task with ID {task_id} not found or already completed"
        )
    return TaskResponse(
        task_id=task_id,
        status="cancelled"
    )

@tasks_router.get("/", response_model=List[Dict[str, Any]])
async def list_tasks():
    """List all tasks and their statuses"""
    return task_manager.list_tasks()
