#!/usr/bin/env python3
"""
DynamoDB Import Monitor Server

A Flask web server to monitor the progress of DynamoDB CSV imports.
Provides a real-time dashboard for tracking import jobs.
"""

import os
import json
import time
from datetime import datetime
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("dynamo_monitor")

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Directory to store progress data
PROGRESS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "progress")
os.makedirs(PROGRESS_DIR, exist_ok=True)

# In-memory cache of job status
job_cache = {}

# Timestamp of last full cache refresh
last_cache_refresh = 0

# Cache refresh interval in seconds
CACHE_REFRESH_INTERVAL = 5


def get_job_files():
    """Get all job progress files."""
    if not os.path.exists(PROGRESS_DIR):
        return []
    return [f for f in os.listdir(PROGRESS_DIR) if f.endswith('.json')]


def load_job_data(job_id):
    """Load job data from file."""
    file_path = os.path.join(PROGRESS_DIR, f"{job_id}.json")
    
    if not os.path.exists(file_path):
        # Remove from cache if it was there
        if job_id in job_cache:
            del job_cache[job_id]
        return None
    
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            # Add metadata for caching
            data['_file_mtime'] = os.path.getmtime(file_path)
            data['_cache_time'] = time.time()
            # Cache the data
            job_cache[job_id] = data
            return data
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from {file_path}")
        return None
    except Exception as e:
        logger.error(f"Error loading job data: {e}")
        return None


def load_all_jobs():
    """Load all job data."""
    global last_cache_refresh, job_cache
    
    current_time = time.time()
    
    # Check if we need to do a full refresh of the cache
    if current_time - last_cache_refresh > CACHE_REFRESH_INTERVAL:
        logger.info("Performing full cache refresh")
        
        # Get all job files
        job_files = get_job_files()
        
        # Track existing job IDs to detect deleted files
        current_job_ids = set()
        
        # Process each job file
        for job_file in job_files:
            job_id = job_file.replace('.json', '')
            current_job_ids.add(job_id)
            
            # Check if we need to refresh this specific job
            file_path = os.path.join(PROGRESS_DIR, job_file)
            file_mtime = os.path.getmtime(file_path)
            
            # Refresh if not in cache or file has been modified
            if job_id not in job_cache or job_cache[job_id].get('_file_mtime', 0) < file_mtime:
                try:
                    with open(file_path, 'r') as f:
                        job_data = json.load(f)
                        job_data['_file_mtime'] = file_mtime
                        job_data['_cache_time'] = current_time
                        
                        # Only cache valid job data
                        if job_data.get('job_id') and job_data.get('table_name') and job_data.get('status'):
                            job_cache[job_id] = job_data
                except Exception as e:
                    logger.error(f"Error loading job data for {job_id}: {e}")
        
        # Remove jobs from cache that no longer exist as files
        for job_id in list(job_cache.keys()):
            if job_id not in current_job_ids:
                del job_cache[job_id]
        
        # Update last refresh time
        last_cache_refresh = current_time
    
    # Return sorted jobs from cache
    jobs = [job for job in job_cache.values() 
            if job.get('job_id') and job.get('table_name') and job.get('status')]
    return sorted(jobs, key=lambda x: x.get('start_time', 0), reverse=True)


@app.route('/')
def index():
    """Render the main dashboard."""
    return render_template('index.html')


@app.route('/api/jobs')
def get_jobs():
    """API endpoint to get all jobs."""
    return jsonify(load_all_jobs())


@app.route('/api/job/<job_id>')
def get_job(job_id):
    """API endpoint to get a specific job."""
    # Check cache first
    if job_id in job_cache:
        # Check if we should refresh from disk
        cache_time = job_cache[job_id].get('_cache_time', 0)
        file_path = os.path.join(PROGRESS_DIR, f"{job_id}.json")
        
        # Only refresh if file exists and has been modified since last cache
        if os.path.exists(file_path):
            file_mtime = os.path.getmtime(file_path)
            if file_mtime > job_cache[job_id].get('_file_mtime', 0):
                # File has been modified, reload it
                try:
                    with open(file_path, 'r') as f:
                        job_data = json.load(f)
                        job_data['_file_mtime'] = file_mtime
                        job_data['_cache_time'] = time.time()
                        job_cache[job_id] = job_data
                except Exception as e:
                    logger.error(f"Error reloading job data for {job_id}: {e}")
            
            return jsonify(job_cache[job_id])
    
    # Not in cache or needs refresh
    job_data = load_job_data(job_id)
    if job_data:
        return jsonify(job_data)
    return jsonify({"error": "Job not found"}), 404


@app.route('/api/job/<job_id>/status')
def get_job_status(job_id):
    """API endpoint to get just the status of a job."""
    job_data = load_job_data(job_id)
    if job_data:
        return jsonify({
            "status": job_data.get("status", "unknown"),
            "processed": job_data.get("processed_items", 0),
            "failed": job_data.get("failed_items", 0),
            "total": job_data.get("total_items", 0),
            "progress": job_data.get("progress_percentage", 0),
            "current_file": job_data.get("current_file", ""),
            "elapsed_time": job_data.get("elapsed_time", 0),
            "estimated_completion": job_data.get("estimated_completion", "unknown")
        })
    return jsonify({"error": "Job not found"}), 404


if __name__ == "__main__":
    # Create templates directory if it doesn't exist
    templates_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
    os.makedirs(templates_dir, exist_ok=True)
    
    # Create the HTML template if it doesn't exist
    template_path = os.path.join(templates_dir, "index.html")
    if not os.path.exists(template_path):
        with open(template_path, 'w') as f:
            f.write("""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DynamoDB Import Monitor</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { padding-top: 20px; }
        .job-card { margin-bottom: 20px; }
        .progress { height: 25px; }
        .progress-bar { line-height: 25px; font-weight: bold; }
        .stats { font-size: 0.9rem; }
        .refresh-btn { margin-bottom: 20px; }
        .job-header { display: flex; justify-content: space-between; align-items: center; }
        .status-badge { font-size: 1rem; }
    </style>
</head>
<body>
    <div class="container">
        <h1 class="mb-4">DynamoDB Import Monitor</h1>
        
        <button id="refresh-btn" class="btn btn-primary refresh-btn">
            <span class="spinner-border spinner-border-sm d-none" role="status" aria-hidden="true"></span>
            Refresh Data
        </button>
        
        <div id="jobs-container" class="row">
            <div class="col-12">
                <div class="alert alert-info">Loading job data...</div>
            </div>
        </div>
    </div>

    <script>
        // Job status colors
        const statusColors = {
            'running': 'primary',
            'completed': 'success',
            'failed': 'danger',
            'pending': 'warning',
            'unknown': 'secondary'
        };

        // Format elapsed time
        function formatTime(seconds) {
            if (!seconds) return 'N/A';
            
            const hrs = Math.floor(seconds / 3600);
            const mins = Math.floor((seconds % 3600) / 60);
            const secs = Math.floor(seconds % 60);
            
            return `${hrs}h ${mins}m ${secs}s`;
        }

        // Store job data globally
        let jobsData = [];
        
        // Load all jobs (full page refresh)
        function loadJobs() {
            const spinner = document.querySelector('#refresh-btn .spinner-border');
            spinner.classList.remove('d-none');
            
            fetch('/api/jobs')
                .then(response => response.json())
                .then(jobs => {
                    jobsData = jobs; // Store jobs globally
                    renderJobs(jobs);
                })
                .catch(error => {
                    console.error('Error fetching jobs:', error);
                    document.getElementById('jobs-container').innerHTML = `
                        <div class="col-12">
                            <div class="alert alert-danger">Error loading job data: ${error.message}</div>
                        </div>
                    `;
                })
                .finally(() => {
                    spinner.classList.add('d-none');
                });
        }
        
        // Update job progress data only (faster updates)
        function updateJobProgress() {
            // Only update if we have jobs
            if (jobsData.length === 0) return;
            
            // For each job, fetch its current status
            jobsData.forEach(job => {
                fetch(`/api/job/${job.job_id}/status`)
                    .then(response => response.json())
                    .then(statusData => {
                        if (statusData.error) return;
                        
                        // Update progress bar
                        const progressBar = document.querySelector(`#progress-${job.job_id}`);
                        if (progressBar) {
                            progressBar.style.width = `${statusData.progress}%`;
                            progressBar.setAttribute('aria-valuenow', statusData.progress);
                            progressBar.textContent = `${statusData.progress}%`;
                        }
                        
                        // Update stats
                        const statsDiv = document.querySelector(`#stats-${job.job_id}`);
                        if (statsDiv) {
                            // Update only the dynamic parts
                            const itemsElement = statsDiv.querySelector('.items-stats');
                            if (itemsElement) {
                                itemsElement.textContent = `${statusData.processed} processed, ${statusData.failed} failed`;
                            }
                            
                            const elapsedElement = statsDiv.querySelector('.elapsed-stats');
                            if (elapsedElement) {
                                elapsedElement.textContent = formatTime(statusData.elapsed_time);
                            }
                            
                            const completionElement = statsDiv.querySelector('.completion-stats');
                            if (completionElement) {
                                completionElement.textContent = statusData.estimated_completion || 'Unknown';
                            }
                        }
                        
                        // Update status badge if status changed
                        if (job.status !== statusData.status) {
                            const badge = document.querySelector(`#status-${job.job_id}`);
                            if (badge) {
                                const statusColor = statusColors[statusData.status] || 'secondary';
                                badge.className = `badge bg-${statusColor} status-badge`;
                                badge.textContent = statusData.status;
                                job.status = statusData.status; // Update in our local data
                            }
                        }
                    })
                    .catch(error => {
                        console.error(`Error updating job ${job.job_id}:`, error);
                    });
            });
        }
        
        // Render all jobs to the page
        function renderJobs(jobs) {
            const container = document.getElementById('jobs-container');
            
            if (jobs.length === 0) {
                container.innerHTML = `
                    <div class="col-12">
                        <div class="alert alert-warning">No import jobs found.</div>
                    </div>
                `;
                return;
            }
            
            let html = '';
            jobs.forEach(job => {
                const statusColor = statusColors[job.status] || 'secondary';
                const startTime = new Date(job.start_time * 1000).toLocaleString();
                
                html += `
                    <div class="col-md-6">
                        <div class="card job-card">
                            <div class="card-header job-header">
                                <h5 class="mb-0">${job.job_id}</h5>
                                <span id="status-${job.job_id}" class="badge bg-${statusColor} status-badge">${job.status}</span>
                            </div>
                            <div class="card-body">
                                <div class="progress mb-3">
                                    <div id="progress-${job.job_id}" class="progress-bar bg-${statusColor}" role="progressbar" 
                                        style="width: ${job.progress_percentage}%;" 
                                        aria-valuenow="${job.progress_percentage}" aria-valuemin="0" aria-valuemax="100">
                                        ${job.progress_percentage}%
                                    </div>
                                </div>
                                
                                <div id="stats-${job.job_id}" class="stats">
                                    <p><strong>Table:</strong> ${job.table_name}</p>
                                    <p><strong>Current File:</strong> ${job.current_file || 'N/A'}</p>
                                    <p><strong>Items:</strong> <span class="items-stats">${job.processed_items} processed, ${job.failed_items} failed</span></p>
                                    <p><strong>Started:</strong> ${startTime}</p>
                                    <p><strong>Elapsed:</strong> <span class="elapsed-stats">${formatTime(job.elapsed_time)}</span></p>
                                    <p><strong>Est. Completion:</strong> <span class="completion-stats">${job.estimated_completion || 'Unknown'}</span></p>
                                </div>
                            </div>
                        </div>
                    </div>
                `;
            });
            
            container.innerHTML = html;
        }

        // Initial load
        document.addEventListener('DOMContentLoaded', loadJobs);
        
        // Refresh button - full refresh
        document.getElementById('refresh-btn').addEventListener('click', loadJobs);
        
        // Fast progress updates every 2 seconds
        setInterval(updateJobProgress, 2000);
        
        // Full page refresh every 15 seconds
        setInterval(loadJobs, 15000);
    </script>
</body>
</html>""")
    
    # Start the server
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
