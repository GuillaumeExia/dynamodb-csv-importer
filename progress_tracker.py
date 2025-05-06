#!/usr/bin/env python3
"""
Progress Tracker for DynamoDB CSV Importer

Tracks and records progress of import jobs for monitoring.
"""

import os
import json
import time
import uuid
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("progress_tracker")

# Directory to store progress data
PROGRESS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "progress")
os.makedirs(PROGRESS_DIR, exist_ok=True)


class ProgressTracker:
    """Tracks progress of DynamoDB import jobs."""
    
    def __init__(self, table_name: str, file_path: Path, total_items: int = 0, job_id: Optional[str] = None):
        """Initialize a new progress tracker."""
        self.job_id = job_id or f"job_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        self.table_name = table_name
        self.file_path = str(file_path)
        self.current_file = os.path.basename(self.file_path)
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.processed_items = 0
        self.failed_items = 0
        self.total_items = total_items
        self.status = "pending"
        self.progress_file = os.path.join(PROGRESS_DIR, f"{self.job_id}.json")
        
        # Create initial progress file
        self._save_progress()
        logger.info(f"Initialized progress tracker for job {self.job_id}")
    
    def start(self) -> None:
        """Mark the job as started."""
        self.status = "running"
        self._save_progress()
        logger.info(f"Job {self.job_id} started")
    
    def update(self, processed: int, failed: int) -> None:
        """Update progress with new counts."""
        self.processed_items += processed
        self.failed_items += failed
        self.last_update_time = time.time()
        self._save_progress()
    
    def complete(self) -> None:
        """Mark the job as completed."""
        self.status = "completed"
        self._save_progress()
        logger.info(f"Job {self.job_id} completed: {self.processed_items} processed, {self.failed_items} failed")
    
    def fail(self, error_message: str) -> None:
        """Mark the job as failed."""
        self.status = "failed"
        self._save_progress(error_message=error_message)
        logger.error(f"Job {self.job_id} failed: {error_message}")
    
    def _calculate_progress(self) -> Dict[str, Any]:
        """Calculate progress statistics."""
        current_time = time.time()
        elapsed_time = current_time - self.start_time
        
        # Calculate progress percentage
        if self.total_items > 0:
            progress_percentage = min(100, round((self.processed_items / self.total_items) * 100, 2))
        else:
            progress_percentage = 0
        
        # Calculate estimated completion time
        estimated_completion = "unknown"
        if self.processed_items > 0 and self.total_items > 0 and progress_percentage < 100:
            items_per_second = self.processed_items / elapsed_time
            if items_per_second > 0:
                remaining_items = self.total_items - self.processed_items
                remaining_seconds = remaining_items / items_per_second
                completion_time = current_time + remaining_seconds
                estimated_completion = datetime.fromtimestamp(completion_time).strftime("%Y-%m-%d %H:%M:%S")
        
        return {
            "progress_percentage": progress_percentage,
            "elapsed_time": round(elapsed_time, 2),
            "estimated_completion": estimated_completion,
            "items_per_second": round(self.processed_items / max(1, elapsed_time), 2) if elapsed_time > 0 else 0
        }
    
    def _save_progress(self, error_message: Optional[str] = None) -> None:
        """Save progress data to file."""
        progress_data = {
            "job_id": self.job_id,
            "table_name": self.table_name,
            "current_file": self.current_file,
            "start_time": self.start_time,
            "last_update_time": self.last_update_time,
            "processed_items": self.processed_items,
            "failed_items": self.failed_items,
            "total_items": self.total_items,
            "status": self.status,
            "_cache_time": time.time()
        }
        
        # Add error message if provided
        if error_message:
            progress_data["error_message"] = error_message
        
        # Add calculated progress metrics
        progress_data.update(self._calculate_progress())
        
        # Save to file
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(progress_data, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving progress data: {e}")


def count_csv_rows(file_path: Path) -> int:
    """Count the number of rows in a CSV file (excluding header)."""
    try:
        with open(file_path, 'r', newline='', encoding='utf-8') as f:
            # Count lines and subtract 1 for header
            return sum(1 for _ in f) - 1
    except Exception as e:
        logger.error(f"Error counting CSV rows: {e}")
        return 0


def get_all_jobs() -> Dict[str, Dict[str, Any]]:
    """Get all job progress data."""
    jobs = {}
    
    if not os.path.exists(PROGRESS_DIR):
        return jobs
    
    for filename in os.listdir(PROGRESS_DIR):
        if filename.endswith('.json'):
            job_id = filename.replace('.json', '')
            try:
                with open(os.path.join(PROGRESS_DIR, filename), 'r') as f:
                    jobs[job_id] = json.load(f)
            except Exception as e:
                logger.error(f"Error loading job data for {job_id}: {e}")
    
    return jobs
