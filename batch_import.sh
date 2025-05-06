#!/bin/bash
#
# Batch processing script for DynamoDB CSV imports
#
# Usage: ./batch_import.sh [options]
#
# Options:
#   --input-file FILE       The large CSV file to process (required)
#   --table-name NAME       DynamoDB table name (required)
#   --schema-file FILE      Path to JSON schema mapping file
#   --chunk-size SIZE       Number of rows per chunk (default: 100000)
#   --batch-size SIZE       Batch size for DynamoDB writes (default: 100)
#   --workers NUM           Number of concurrent workers (default: 20)
#   --region REGION         AWS region
#   --profile PROFILE       AWS profile name
#   --no-monitor            Disable progress monitoring
#   --help                  Show this help message

# Default values
CHUNK_SIZE=100000
BATCH_SIZE=100
WORKERS=20
NO_MONITOR=false

# Function to display help
show_help() {
    echo "Batch processing script for DynamoDB CSV imports"
    echo ""
    echo "Usage: ./batch_import.sh [options]"
    echo ""
    echo "Options:"
    echo "  --input-file FILE       The large CSV file to process (required)"
    echo "  --table-name NAME       DynamoDB table name (required)"
    echo "  --schema-file FILE      Path to JSON schema mapping file"
    echo "  --chunk-size SIZE       Number of rows per chunk (default: 100000)"
    echo "  --batch-size SIZE       Batch size for DynamoDB writes (default: 100)"
    echo "  --workers NUM           Number of concurrent workers (default: 20)"
    echo "  --region REGION         AWS region"
    echo "  --profile PROFILE       AWS profile name"
    echo "  --no-monitor            Disable progress monitoring"
    echo "  --help                  Show this help message"
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --input-file)
            INPUT_FILE="$2"
            shift 2
            ;;
        --table-name)
            TABLE_NAME="$2"
            shift 2
            ;;
        --schema-file)
            SCHEMA_FILE="$2"
            shift 2
            ;;
        --chunk-size)
            CHUNK_SIZE="$2"
            shift 2
            ;;
        --batch-size)
            BATCH_SIZE="$2"
            shift 2
            ;;
        --workers)
            WORKERS="$2"
            shift 2
            ;;
        --region)
            REGION="$2"
            shift 2
            ;;
        --profile)
            PROFILE="$2"
            shift 2
            ;;
        --no-monitor)
            NO_MONITOR=true
            shift
            ;;
        --help)
            show_help
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            ;;
    esac
done

# Check required parameters
if [ -z "$INPUT_FILE" ]; then
    echo "Error: Input file is required (--input-file)"
    exit 1
fi

if [ -z "$TABLE_NAME" ]; then
    echo "Error: Table name is required (--table-name)"
    exit 1
fi

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Create chunks directory
CHUNKS_DIR="$SCRIPT_DIR/chunks"
if [ ! -d "$CHUNKS_DIR" ]; then
    mkdir -p "$CHUNKS_DIR"
    echo "Created chunks directory: $CHUNKS_DIR"
fi

# Create progress directory
PROGRESS_DIR="$SCRIPT_DIR/progress"
if [ ! -d "$PROGRESS_DIR" ]; then
    mkdir -p "$PROGRESS_DIR"
    echo "Created progress directory: $PROGRESS_DIR"
fi

# Convert relative paths to absolute paths
if [[ ! "$INPUT_FILE" = /* ]]; then
    INPUT_FILE="$(pwd)/$INPUT_FILE"
fi

if [ -n "$SCHEMA_FILE" ] && [[ ! "$SCHEMA_FILE" = /* ]]; then
    SCHEMA_FILE="$(pwd)/$SCHEMA_FILE"
fi

# Check if input file exists
if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: Input file not found: $INPUT_FILE"
    exit 1
fi

# Tracking file for processed chunks
TRACKING_FILE="$PROGRESS_DIR/batch_progress.json"
PROCESSED_CHUNKS=()

# Load tracking data if it exists
if [ -f "$TRACKING_FILE" ]; then
    if command -v jq &> /dev/null; then
        # Use jq if available
        PROCESSED_CHUNKS=($(jq -r '.processed_chunks[]' "$TRACKING_FILE" 2>/dev/null))
        echo "Found tracking data with ${#PROCESSED_CHUNKS[@]} processed chunks"
    else
        # Fallback to grep/sed if jq is not available
        echo "Warning: jq not found, using basic parsing for tracking file"
        PROCESSED_CHUNKS=($(grep -o '"chunk_[0-9]\+\.csv"' "$TRACKING_FILE" | sed 's/"//g'))
        echo "Found tracking data with ${#PROCESSED_CHUNKS[@]} processed chunks"
    fi
fi

# Split the large CSV file into chunks
echo "Splitting $INPUT_FILE into chunks of $CHUNK_SIZE rows each..."

# Get the header
HEADER=$(head -n 1 "$INPUT_FILE")

# Split the file
LINE_COUNT=0
FILE_NUMBER=1
CHUNK_FILE="$CHUNKS_DIR/chunk_$FILE_NUMBER.csv"

# Write header to first chunk file
echo "$HEADER" > "$CHUNK_FILE"
echo "Creating chunk file: $CHUNK_FILE"

# Process the file line by line, skipping the header
tail -n +2 "$INPUT_FILE" | while IFS= read -r LINE; do
    echo "$LINE" >> "$CHUNK_FILE"
    LINE_COUNT=$((LINE_COUNT + 1))
    
    if [ $LINE_COUNT -eq $CHUNK_SIZE ]; then
        FILE_NUMBER=$((FILE_NUMBER + 1))
        CHUNK_FILE="$CHUNKS_DIR/chunk_$FILE_NUMBER.csv"
        echo "Creating chunk file: $CHUNK_FILE"
        echo "$HEADER" > "$CHUNK_FILE"
        LINE_COUNT=0
    fi
done

echo "Created $FILE_NUMBER chunk files in $CHUNKS_DIR"

# Start the monitor server in the background
MONITOR_SERVER_PID=""
if [ "$NO_MONITOR" = false ]; then
    echo "Starting monitor server..."
    
    # Check for Python in virtual environment first
    PYTHON_PATH="$SCRIPT_DIR/venv/bin/python"
    if [ -f "$PYTHON_PATH" ]; then
        PYTHON_EXE="$PYTHON_PATH"
    else
        # Fall back to system Python if venv not found
        PYTHON_EXE="python3"
    fi
    
    MONITOR_SCRIPT="$SCRIPT_DIR/monitor_server.py"
    
    # Start the monitor server
    "$PYTHON_EXE" "$MONITOR_SCRIPT" &
    MONITOR_SERVER_PID=$!
    
    if [ $? -eq 0 ]; then
        echo "Monitor server started with PID: $MONITOR_SERVER_PID"
        echo "Monitor your imports at: http://localhost:5000"
        # Give the server a moment to start
        sleep 2
    else
        echo "Warning: Failed to start monitor server"
    fi
fi

# Process each chunk
CHUNKS=($(ls -1 "$CHUNKS_DIR"/chunk_*.csv | sort))
TOTAL_CHUNKS=${#CHUNKS[@]}
CURRENT_CHUNK=0

for CHUNK in "${CHUNKS[@]}"; do
    CURRENT_CHUNK=$((CURRENT_CHUNK + 1))
    CHUNK_NAME=$(basename "$CHUNK")
    
    # Skip already processed chunks
    if [[ " ${PROCESSED_CHUNKS[@]} " =~ " ${CHUNK_NAME} " ]]; then
        echo "Skipping already processed chunk: $CHUNK_NAME [$CURRENT_CHUNK of $TOTAL_CHUNKS]"
        continue
    fi
    
    echo "Processing $CHUNK_NAME [$CURRENT_CHUNK of $TOTAL_CHUNKS]..."
    
    # Build command arguments
    ARGS=("dynamodb_csv_importer.py" "--table" "$TABLE_NAME" "--file" "$CHUNK" "--batch-size" "$BATCH_SIZE" "--workers" "$WORKERS")
    
    if [ -n "$SCHEMA_FILE" ]; then
        ARGS+=("--schema" "$SCHEMA_FILE")
    fi
    
    if [ -n "$REGION" ]; then
        ARGS+=("--region" "$REGION")
    fi
    
    if [ -n "$PROFILE" ]; then
        ARGS+=("--profile" "$PROFILE")
    fi
    
    if [ "$NO_MONITOR" = true ]; then
        ARGS+=("--no-monitor")
    fi
    
    # Generate a job ID that includes the chunk name
    JOB_ID="batch_${CHUNK_NAME%.csv}"
    ARGS+=("--job-id" "$JOB_ID")
    
    # Execute the import command
    if [ -f "$SCRIPT_DIR/venv/bin/python" ]; then
        PYTHON_EXE="$SCRIPT_DIR/venv/bin/python"
    else
        # Fall back to system Python if venv not found
        PYTHON_EXE="python3"
    fi
    
    COMMAND="$PYTHON_EXE ${ARGS[@]}"
    echo "Executing: $COMMAND"
    
    START_TIME=$(date +%s)
    "$PYTHON_EXE" "${ARGS[@]}"
    EXIT_CODE=$?
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    
    # Check result
    if [ $EXIT_CODE -eq 0 ]; then
        echo "Successfully processed $CHUNK_NAME in $DURATION seconds"
        
        # Update tracking file
        PROCESSED_CHUNKS+=("$CHUNK_NAME")
        PROGRESS_PERCENTAGE=$(awk "BEGIN {printf \"%.2f\", (${#PROCESSED_CHUNKS[@]} / $TOTAL_CHUNKS) * 100}")
        
        # Create JSON tracking data
        if command -v jq &> /dev/null; then
            # Use jq if available
            echo "{\"last_updated\": \"$(date -Iseconds)\", \"processed_chunks\": $(jq -n --argjson chunks "$(printf '%s\n' "${PROCESSED_CHUNKS[@]}" | jq -R . | jq -s .)" '$chunks'), \"total_chunks\": $TOTAL_CHUNKS, \"progress_percentage\": $PROGRESS_PERCENTAGE}" > "$TRACKING_FILE"
        else
            # Fallback to basic JSON creation
            JSON_CHUNKS="["
            for i in "${!PROCESSED_CHUNKS[@]}"; do
                if [ $i -gt 0 ]; then
                    JSON_CHUNKS+=", "
                fi
                JSON_CHUNKS+="\"${PROCESSED_CHUNKS[$i]}\""
            done
            JSON_CHUNKS+="]"
            
            echo "{\"last_updated\": \"$(date -Iseconds)\", \"processed_chunks\": $JSON_CHUNKS, \"total_chunks\": $TOTAL_CHUNKS, \"progress_percentage\": $PROGRESS_PERCENTAGE}" > "$TRACKING_FILE"
        fi
    else
        echo "Error: Failed to process $CHUNK_NAME (Exit code: $EXIT_CODE)"
        # Continue with next chunk even if this one failed
    fi
    
    # Optional delay between chunks to avoid throttling
    sleep 2
done

# Clean up
if [ -n "$MONITOR_SERVER_PID" ]; then
    echo "Stopping monitor server..."
    kill $MONITOR_SERVER_PID 2>/dev/null
fi

echo "Batch processing complete!"
echo "Processed ${#PROCESSED_CHUNKS[@]} of $TOTAL_CHUNKS chunks"

if [ ${#PROCESSED_CHUNKS[@]} -eq $TOTAL_CHUNKS ]; then
    echo -e "\033[0;32mAll chunks were processed successfully!\033[0m"
else
    REMAINING=$((TOTAL_CHUNKS - ${#PROCESSED_CHUNKS[@]}))
    echo -e "\033[0;33m$REMAINING chunks were not processed\033[0m"
fi

# Make script executable
chmod +x "$0"
