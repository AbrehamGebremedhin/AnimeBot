import asyncio
import logging
import traceback
from typing import Callable, Dict, Any, List, Optional, Coroutine
from datetime import datetime

logger = logging.getLogger(__name__)

class BackgroundTaskManager:
    """
    Manages asynchronous background tasks to prevent blocking the API
    """
    def __init__(self, max_workers: int = 10):
        self.max_workers = max_workers
        self.tasks: Dict[str, asyncio.Task] = {}
        self.results: Dict[str, Any] = {}
        self.task_semaphore = asyncio.Semaphore(max_workers)
        
    async def add_task(
        self, 
        task_id: str, 
        coroutine: Coroutine,
        timeout: Optional[float] = None
    ) -> str:
        """
        Add a task to be executed in the background
        
        Args:
            task_id: Unique identifier for the task
            coroutine: The coroutine to execute
            timeout: Optional timeout in seconds
            
        Returns:
            task_id: The task ID for later retrieval
        """
        if task_id in self.tasks and not self.tasks[task_id].done():
            # Task with this ID already exists and is running
            return task_id
            
        # Create a wrapper that uses the semaphore
        async def task_wrapper():
            try:
                async with self.task_semaphore:
                    if timeout:
                        try:
                            self.results[task_id] = {
                                "status": "running",
                                "start_time": datetime.utcnow().isoformat()
                            }
                            result = await asyncio.wait_for(coroutine, timeout=timeout)
                            self.results[task_id] = {
                                "status": "completed",
                                "result": result,
                                "end_time": datetime.utcnow().isoformat()
                            }
                        except asyncio.TimeoutError:
                            self.results[task_id] = {
                                "status": "timeout",
                                "error": "Task exceeded time limit",
                                "end_time": datetime.utcnow().isoformat()
                            }
                    else:
                        self.results[task_id] = {
                            "status": "running",
                            "start_time": datetime.utcnow().isoformat()
                        }
                        result = await coroutine
                        self.results[task_id] = {
                            "status": "completed",
                            "result": result,
                            "end_time": datetime.utcnow().isoformat()
                        }
            except Exception as e:
                logger.error(f"Background task {task_id} failed: {str(e)}")
                logger.error(traceback.format_exc())
                self.results[task_id] = {
                    "status": "error",
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                    "end_time": datetime.utcnow().isoformat()
                }
            finally:
                # Clean up the task from the tasks dict when done
                if task_id in self.tasks:
                    del self.tasks[task_id]
                    
        # Create and store the task
        task = asyncio.create_task(task_wrapper())
        self.tasks[task_id] = task
        
        return task_id
        
    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """
        Get the status of a background task
        
        Args:
            task_id: The ID of the task to check
            
        Returns:
            dict: Task status information
        """
        if task_id in self.results:
            return self.results[task_id]
        elif task_id in self.tasks:
            return {
                "status": "running",
                "start_time": None  # Would be populated in a real implementation
            }
        else:
            return {
                "status": "not_found",
                "error": f"No task found with ID {task_id}"
            }
    
    def cancel_task(self, task_id: str) -> bool:
        """
        Cancel a running background task
        
        Args:
            task_id: The ID of the task to cancel
            
        Returns:
            bool: True if task was canceled, False otherwise
        """
        if task_id in self.tasks and not self.tasks[task_id].done():
            self.tasks[task_id].cancel()
            self.results[task_id] = {
                "status": "cancelled",
                "end_time": datetime.utcnow().isoformat()
            }
            return True
        return False
    
    def list_tasks(self) -> List[Dict[str, Any]]:
        """
        List all tasks and their statuses
        
        Returns:
            list: List of task information dictionaries
        """
        tasks_info = []
        
        # Running tasks
        for task_id, task in self.tasks.items():
            status = "running"
            if task.done():
                if task.cancelled():
                    status = "cancelled"
                elif task.exception() is not None:
                    status = "error"
                else:
                    status = "completed"
                    
            tasks_info.append({
                "task_id": task_id,
                "status": status
            })
            
        # Tasks with results but no longer in the tasks dict
        for task_id, result in self.results.items():
            if task_id not in self.tasks:
                tasks_info.append({
                    "task_id": task_id,
                    "status": result.get("status", "unknown"),
                    "end_time": result.get("end_time")
                })
                
        return tasks_info
        
    async def cleanup(self, max_age_seconds: int = 3600):
        """
        Clean up old completed task results
        
        Args:
            max_age_seconds: Maximum age of completed tasks to keep
        """
        now = datetime.utcnow()
        keys_to_remove = []
        
        for task_id, result in self.results.items():
            if result.get("status") in ["completed", "error", "cancelled", "timeout"]:
                if "end_time" in result:
                    try:
                        end_time = datetime.fromisoformat(result["end_time"])
                        age = (now - end_time).total_seconds()
                        if age > max_age_seconds:
                            keys_to_remove.append(task_id)
                    except (ValueError, TypeError):
                        # If the end_time isn't a valid datetime, keep it for now
                        pass
                        
        for task_id in keys_to_remove:
            del self.results[task_id]
            
        return len(keys_to_remove)

# Create a singleton instance
task_manager = BackgroundTaskManager()
