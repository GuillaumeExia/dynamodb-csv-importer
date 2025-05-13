#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Batch processing script for DynamoDB CSV imports
.DESCRIPTION
    Processes large CSV files by splitting them into chunks and importing them sequentially
.PARAMETER InputFile
    The large CSV file to process
.PARAMETER ChunkSize
    Number of rows per chunk (default: 100000)
.PARAMETER TableName
    DynamoDB table name
.PARAMETER SchemaFile
    Path to JSON schema mapping file
.PARAMETER BatchSize
    Batch size for DynamoDB writes (default: 100)
.PARAMETER Workers
    Number of concurrent workers (default: 20)
.PARAMETER Region
    AWS region
.PARAMETER Profile
    AWS profile name
.PARAMETER NoMonitor
    Disable progress monitoring
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$InputFile,
    
    [Parameter(Mandatory=$true)]
    [string]$TableName,
    
    [Parameter(Mandatory=$false)]
    [string]$SchemaFile,
    
    [Parameter(Mandatory=$false)]
    [int]$ChunkSize = 100000,
    
    [Parameter(Mandatory=$false)]
    [int]$BatchSize = 100,
    
    [Parameter(Mandatory=$false)]
    [int]$Workers = 20,
    
    [Parameter(Mandatory=$false)]
    [string]$Region,
    
    [Parameter(Mandatory=$false)]
    [string]$Profile,
    
    [Parameter(Mandatory=$false)]
    [switch]$NoMonitor
)

# Create chunks directory
$chunksDir = Join-Path $PSScriptRoot "chunks"
if (-not (Test-Path $chunksDir)) {
    New-Item -Path $chunksDir -ItemType Directory | Out-Null
    Write-Host "Created chunks directory: $chunksDir"
}

# Create progress directory
$progressDir = Join-Path $PSScriptRoot "progress"
if (-not (Test-Path $progressDir)) {
    New-Item -Path $progressDir -ItemType Directory | Out-Null
    Write-Host "Created progress directory: $progressDir"
}

# Convert relative paths to absolute paths
# First, check if the path is relative (doesn't have a drive letter)
if (-not [System.IO.Path]::IsPathRooted($InputFile)) {
    # Convert relative path to absolute path based on current directory
    $InputFile = Join-Path (Get-Location).Path $InputFile
}

if ($SchemaFile -and -not [System.IO.Path]::IsPathRooted($SchemaFile)) {
    $SchemaFile = Join-Path (Get-Location).Path $SchemaFile
}

# Check if input file exists
if (-not (Test-Path $InputFile)) {
    Write-Error "Input file not found: $InputFile"
    exit 1
}

# Validate schema before processing if schema file is provided
if ($SchemaFile) {
    Write-Host "Validating schema against CSV file before processing..."
    $pythonPath = Join-Path $PSScriptRoot "venv\Scripts\python.exe"
    if (Test-Path $pythonPath) {
        $pythonExe = $pythonPath
    } else {
        # Fall back to system Python if venv not found
        $pythonExe = "python"
    }
    
    $validateScript = Join-Path $PSScriptRoot "validate_schema.py"
    
    $validateArgs = @(
        $validateScript,
        "--file", $InputFile,
        "--schema", $SchemaFile
    )
    
    if ($Encoding) {
        $validateArgs += @("--encoding", $Encoding)
    }
    
    $validateProcess = Start-Process -FilePath $pythonExe -ArgumentList $validateArgs -Wait -NoNewWindow -PassThru
    
    if ($validateProcess.ExitCode -ne 0) {
        Write-Error "Schema validation failed. Please check your schema and CSV file."
        exit 1
    }
    
    Write-Host "Schema validation successful! Proceeding with import..."
}

# Count total rows in the CSV file (excluding header)
$totalRows = (Get-Content $InputFile).Count - 1  # Subtract 1 for header
Write-Host "Total rows in CSV: $totalRows"

# Calculate number of chunks
$numChunks = [Math]::Ceiling($totalRows / $ChunkSize)
Write-Host "Will create $numChunks chunks"

# Check for existing chunks
$existingChunks = @()
if (Test-Path $chunksDir) {
    $existingChunks = Get-ChildItem -Path $chunksDir -Filter "chunk_*.csv" | Select-Object -ExpandProperty Name
    if ($existingChunks.Count -gt 0) {
        Write-Host "Found $($existingChunks.Count) existing chunk files"
    }
}

# Check if we need to create chunks
if ($existingChunks.Count -eq $numChunks) {
    Write-Host "All $numChunks chunks already exist. Skipping chunk creation."
}
else {
    # Split the large CSV file into chunks
    Write-Host "Splitting $InputFile into chunks of $ChunkSize rows each..."

    # Read the header
    $header = Get-Content $InputFile -TotalCount 1

    # Create chunks
    for ($i = 1; $i -le $numChunks; $i++) {
        $chunkFile = Join-Path $chunksDir "chunk_$i.csv"
        
        # Skip if this chunk file already exists
        if (Test-Path $chunkFile) {
            Write-Host "Skipping chunk_$i.csv (file already exists)"
            continue
        }
        
        # Calculate start and end lines for this chunk
        $startLine = (($i-1) * $ChunkSize) + 2  # +2 because line 1 is header and PowerShell is 1-indexed
        $endLine = $startLine + $ChunkSize - 1
        
        # Make sure we don't exceed the file
        if ($endLine -gt ($totalRows + 1)) {
            $endLine = $totalRows + 1
        }
        
        # Create the chunk file with header
        $header | Set-Content $chunkFile
        
        # Add the data rows
        Get-Content $InputFile | Select-Object -Index ($startLine-1)..($endLine-1) | Add-Content $chunkFile
        
        Write-Host "Created $chunkFile with rows $(($startLine-1))-$(($endLine-1))"
    }
}

# Create tracking file for processed chunks
$trackingFile = Join-Path $progressDir "batch_progress.json"
$processedChunks = @()

if (Test-Path $trackingFile) {
    try {
        $trackingData = Get-Content $trackingFile -Raw | ConvertFrom-Json
        $processedChunks = $trackingData.processed_chunks
        Write-Host "Found tracking data with $($processedChunks.Count) processed chunks"
    }
    catch {
        Write-Warning "Error reading tracking file: $_"
        $processedChunks = @()
    }
}

# Get list of all chunk files
$chunkFiles = Get-ChildItem -Path $chunksDir -Filter "chunk_*.csv" | Sort-Object Name

# Start the monitor server in the background
$monitorServerProcess = $null
if (-not $NoMonitor) {
    Write-Host "Starting monitor server..."
    $pythonPath = Join-Path $PSScriptRoot "venv\Scripts\python.exe"
    if (Test-Path $pythonPath) {
        $pythonExe = $pythonPath
    } else {
        # Fall back to system Python if venv not found
        $pythonExe = "python"
    }
    $monitorScript = Join-Path $PSScriptRoot "monitor_server.py"
    
    try {
        $monitorServerProcess = Start-Process -FilePath $pythonExe -ArgumentList $monitorScript -PassThru -WindowStyle Hidden
        Write-Host "Monitor server started with PID: $($monitorServerProcess.Id)"
        Write-Host "Monitor your imports at: http://localhost:5000"
        # Give the server a moment to start
        Start-Sleep -Seconds 2
    }
    catch {
        Write-Warning "Failed to start monitor server: $_"
    }
}

# Process each chunk
# Sort chunks numerically by extracting the number from chunk_N.csv and sorting
$chunks = Get-ChildItem -Path $chunksDir -Filter "chunk_*.csv" | Sort-Object { 
    # Extract the numeric part from chunk_N.csv and convert to integer for proper numeric sorting
    [int]($_.Name -replace 'chunk_([0-9]+)\.csv', '$1')
}
$totalChunks = $chunks.Count
$currentChunk = 0

foreach ($chunk in $chunks) {
    $currentChunk++
    
    # Skip already processed chunks
    if ($processedChunks -contains $chunk.Name) {
        Write-Host "Skipping already processed chunk: $($chunk.Name) [$currentChunk of $totalChunks]"
        continue
    }
    
    Write-Host "Processing $($chunk.Name) [$currentChunk of $totalChunks]..."
    
    # Build command arguments
    $args = @(
        "dynamodb_csv_importer.py",
        "--table", $TableName,
        "--file", $chunk.FullName,
        "--batch-size", $BatchSize,
        "--workers", $Workers
    )
    
    if ($SchemaFile) {
        $args += @("--schema", $SchemaFile)
    }
    
    if ($Region) {
        $args += @("--region", $Region)
    }
    
    if ($Profile) {
        $args += @("--profile", $Profile)
    }
    
    if ($NoMonitor) {
        $args += "--no-monitor"
    }
    
    # Generate a job ID that includes the chunk name
    $jobId = "batch_" + $chunk.Name.Replace(".csv", "")
    $args += @("--job-id", $jobId)
    
    # Execute the import command
    $pythonPath = Join-Path $PSScriptRoot "venv\Scripts\python.exe"
    if (Test-Path $pythonPath) {
        $pythonExe = $pythonPath
    } else {
        # Fall back to system Python if venv not found
        $pythonExe = "python"
    }
    $argsStr = $args -join " "
    $command = "$pythonExe $argsStr"
    Write-Host "Executing: $command"
    
    $startTime = Get-Date
    $process = Start-Process -FilePath $pythonExe -ArgumentList $args -Wait -NoNewWindow -PassThru
    $endTime = Get-Date
    $duration = ($endTime - $startTime).TotalSeconds
    
    # Check result
    if ($process.ExitCode -eq 0) {
        Write-Host "Successfully processed $($chunk.Name) in $duration seconds"
        
        # Update tracking file
        $processedChunks += $chunk.Name
        $trackingData = @{
            "last_updated" = (Get-Date).ToString("o")
            "processed_chunks" = $processedChunks
            "total_chunks" = $totalChunks
            "progress_percentage" = [math]::Round(($processedChunks.Count / $totalChunks) * 100, 2)
        }
        
        $trackingData | ConvertTo-Json | Set-Content -Path $trackingFile
    }
    else {
        Write-Error "Failed to process $($chunk.Name) (Exit code: $($process.ExitCode))"
        # Continue with next chunk even if this one failed
    }
    
    # Optional delay between chunks to avoid throttling
    Start-Sleep -Seconds 2
}

# Clean up
if ($null -ne $monitorServerProcess -and -not $monitorServerProcess.HasExited) {
    Write-Host "Stopping monitor server..."
    Stop-Process -Id $monitorServerProcess.Id -Force
}

Write-Host "Batch processing complete!"
Write-Host "Processed $($processedChunks.Count) of $totalChunks chunks"

if ($processedChunks.Count -eq $totalChunks) {
    Write-Host "All chunks were processed successfully!" -ForegroundColor Green
}
else {
    Write-Host "$($totalChunks - $processedChunks.Count) chunks were not processed" -ForegroundColor Yellow
}
