#!/usr/bin/env python3
"""
DynamoDB CSV Data Importer

A script to import CSV data into a DynamoDB table with proper error handling,
logging, configuration management, and performance optimization.
Supports configurable schema mapping for CSV to DynamoDB conversion.
"""

import sys
import csv
import json
import logging
import argparse
import os
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List, Union, Callable, Tuple
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# Import progress tracker
try:
    from progress_tracker import ProgressTracker, count_csv_rows
except ImportError:
    # Define fallback classes/functions if module not available
    class ProgressTracker:
        def __init__(self, *args, **kwargs):
            pass
        def start(self): pass
        def update(self, processed, failed): pass
        def complete(self): pass
        def fail(self, error_message): pass
    
    def count_csv_rows(file_path):
        return 0


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("dynamo_importer")


# Type definitions for schema
FieldType = str  # 'S', 'N', 'B', 'BOOL', 'NULL', 'M', 'L', 'SS', 'NS', 'BS'
NestedSchema = Dict[str, Union[str, 'SchemaMapping']]  # For nested attributes
SchemaMapping = Dict[str, Union[str, NestedSchema]]  # Maps CSV field -> DynamoDB field


@dataclass
class Config:
    """Configuration for the DynamoDB importer."""
    table_name: str
    csv_file: Path
    schema_file: Optional[Path] = None
    schema: Dict[str, Any] = field(default_factory=dict)
    hash_key: str = ""
    range_key: Optional[str] = None
    batch_size: int = 25
    encoding: str = "utf-8-sig"  # Default encoding with BOM handling
    max_workers: int = 10
    region: Optional[str] = None
    profile: Optional[str] = None
    job_id: Optional[str] = None
    monitor: bool = True


def parse_arguments() -> Config:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Import CSV data into DynamoDB")
    parser.add_argument("--table", required=True, help="DynamoDB table name")
    parser.add_argument("--file", required=True, type=Path, help="Path to CSV file")
    parser.add_argument("--schema", type=Path, help="Path to JSON schema mapping file")
    parser.add_argument("--hash-key", help="Name of the hash key in DynamoDB")
    parser.add_argument("--range-key", help="Name of the range key in DynamoDB (if any)")
    parser.add_argument("--batch-size", type=int, default=25, help="Batch size for DynamoDB writes")
    parser.add_argument("--workers", type=int, default=10, help="Number of concurrent workers")
    parser.add_argument("--region", help="AWS region")
    parser.add_argument("--profile", help="AWS profile name")
    parser.add_argument("--job-id", help="Custom job ID for progress tracking")
    parser.add_argument("--no-monitor", action="store_true", help="Disable progress monitoring")
    parser.add_argument("--encoding", default="utf-8-sig", help="CSV file encoding (default: utf-8-sig, will fallback to latin-1 if needed)")
    
    args = parser.parse_args()
    
    # Validate file exists
    if not args.file.exists():
        parser.error(f"File not found: {args.file}")
    
    config = Config(
        table_name=args.table,
        csv_file=args.file,
        schema_file=args.schema,
        batch_size=args.batch_size,
        max_workers=args.workers,
        region=args.region,
        profile=args.profile,
        encoding=args.encoding,
    )
    
    # Load schema if provided
    if args.schema:
        if not args.schema.exists():
            parser.error(f"Schema file not found: {args.schema}")
        
        try:
            with open(args.schema, 'r') as f:
                config.schema = json.load(f)
                logger.info(f"Loaded schema from {args.schema}")
        except json.JSONDecodeError as e:
            parser.error(f"Invalid JSON in schema file: {e}")
        except Exception as e:
            parser.error(f"Error reading schema file: {e}")
    
    # Set hash and range keys
    if args.hash_key:
        config.hash_key = args.hash_key
    elif not config.schema.get('hash_key'):
        if not args.schema:
            parser.error("Hash key must be specified with --hash-key or in schema file")
        else:
            parser.error("Missing 'hash_key' in schema file")
    else:
        config.hash_key = config.schema.get('hash_key')
    
    if args.range_key:
        config.range_key = args.range_key
    elif config.schema.get('range_key'):
        config.range_key = config.schema.get('range_key')
    
    return config


def get_dynamodb_resource(config: Config) -> boto3.resource:
    """Create and return a configured DynamoDB resource."""
    import botocore.config
    
    # Create a custom configuration with increased max pool connections
    # Set max_pool_connections to match or exceed the number of workers
    boto_config = botocore.config.Config(
        max_pool_connections=config.max_workers * 2,  # Double the worker count for safety
        retries={
            'max_attempts': 10,  # Increase retry attempts
            'mode': 'adaptive'
        }
    )
    
    session_kwargs = {}
    
    if config.profile:
        session_kwargs["profile_name"] = config.profile
    
    if config.region:
        session_kwargs["region_name"] = config.region
    
    session = boto3.Session(**session_kwargs)
    return session.resource("dynamodb", config=boto_config)


def read_csv_data(file_path: Path, encoding: str = "utf-8-sig") -> Iterator[Dict[str, str]]:
    """Read data from a CSV file."""
    encodings_to_try = [encoding, "latin-1", "cp1252", "iso-8859-1"]
    last_error = None
    
    # Try different encodings until one works
    for enc in encodings_to_try:
        try:
            with open(file_path, 'r', newline='', encoding=enc) as f:
                reader = csv.DictReader(f)
                
                # Normalize fieldnames to handle BOM and other encoding issues
                if reader.fieldnames:
                    # Log original headers for debugging
                    logger.info(f"Original CSV headers: {', '.join(reader.fieldnames)}")
                    logger.info(f"Successfully opened CSV with encoding: {enc}")
                    
                    # Create a normalized version of the CSV data with clean keys
                    for row in reader:
                        # Preserve the original data but ensure we can access it with clean keys
                        normalized_row = {}
                        for key, value in row.items():
                            # Keep original key-value pair
                            normalized_row[key] = value
                            
                            # Also add a normalized version if different
                            clean_key = key
                            # Remove BOM if present
                            if clean_key.startswith('\ufeff'):
                                clean_key = clean_key[1:]
                                normalized_row[clean_key] = value
                        
                        yield normalized_row
                    return  # Successfully read the file, exit the function
                else:
                    logger.warning("CSV file has no headers")
                    return
        except (UnicodeDecodeError, IOError) as e:
            last_error = e
            logger.debug(f"Failed to open CSV with encoding {enc}: {e}")
            continue  # Try the next encoding
        except csv.Error as e:
            logger.error(f"CSV parsing error: {e}")
            raise
    
    # If we get here, all encodings failed
    logger.error(f"Error reading CSV file with all attempted encodings. Last error: {last_error}")
    raise last_error


def process_batch(table, items: List[Dict[str, Any]]) -> Tuple[int, int]:
    """Process a batch of items to DynamoDB."""
    successful = 0
    failed = 0

    with table.batch_writer() as batch:
        for item in items:
            try:
                batch.put_item(Item=item)
                successful += 1
            except ClientError as e:
                logger.error(f"Failed to write item: {e}")
                failed += 1
    
    return successful, failed


def type_converter(value: str, field_type: str) -> Any:
    """Convert string value from CSV to appropriate DynamoDB type."""
    if not value:  # Handle empty values
        if field_type == 'NULL':
            return True  # DynamoDB NULL type
        return None  # Skip this attribute
    
    try:
        if field_type == 'S':  # String
            return value
        elif field_type == 'N':  # Number
            # Use Decimal instead of float for DynamoDB compatibility
            return Decimal(value)
        elif field_type == 'BOOL':  # Boolean
            return value.lower() in ('true', 'yes', '1', 'y')
        elif field_type == 'B':  # Binary (base64)
            import base64
            return base64.b64decode(value)
        elif field_type.startswith('L'):  # List type with subtype (e.g., 'L:S', 'L:N')
            subtype = field_type.split(':')[1] if ':' in field_type else 'S'
            items = [item.strip() for item in value.split(',')]
            return [type_converter(item, subtype) for item in items if item]
        elif field_type.startswith('M'):  # Map type (simple key-value)
            import json
            return json.loads(value)
        elif field_type == 'SS':  # String Set
            return set(item.strip() for item in value.split(',') if item.strip())
        elif field_type == 'NS':  # Number Set
            # Use Decimal for number sets as well
            return set(Decimal(item.strip()) for item in value.split(',') if item.strip())
        else:
            return value  # Default to string
    except (ValueError, TypeError) as e:
        logger.warning(f"Type conversion error for value '{value}' to type {field_type}: {e}")
        return None


def build_nested_structure(csv_row: Dict[str, str], mapping: Dict[str, Any]) -> Dict[str, Any]:
    """Build a nested DynamoDB item structure based on schema mapping."""
    result = {}
    
    # Process each field in the mapping
    for dynamo_field, field_info in mapping.items():
        if isinstance(field_info, dict):  # Nested structure
            if 'type' in field_info and field_info['type'] == 'M':  # Map type
                result[dynamo_field] = build_nested_structure(csv_row, field_info['fields'])
            else:  # Regular nested structure
                result[dynamo_field] = build_nested_structure(csv_row, field_info)
        else:  # Simple field mapping: "dynamo_field": "csv_field:type"
            csv_field, *type_info = field_info.split(':')
            field_type = type_info[0] if type_info else 'S'  # Default to string type
            
            # Try to find the field with various normalization techniques
            value = None
            
            # 1. Try exact match
            if csv_field in csv_row:
                value = type_converter(csv_row[csv_field], field_type)
            else:
                # 2. Try removing BOM if present
                if csv_field.startswith('\ufeff'):
                    clean_field = csv_field[1:]
                    if clean_field in csv_row:
                        value = type_converter(csv_row[clean_field], field_type)
                
                # 3. Try adding BOM if not present
                if value is None and not csv_field.startswith('\ufeff'):
                    bom_field = '\ufeff' + csv_field
                    if bom_field in csv_row:
                        value = type_converter(csv_row[bom_field], field_type)
                
                # 4. Try normalized keys (strip whitespace, lowercase)
                if value is None:
                    normalized_field = csv_field.strip().lower()
                    for key in csv_row:
                        if key.strip().lower() == normalized_field:
                            value = type_converter(csv_row[key], field_type)
                            break
            
            if value is not None:  # Skip None values
                result[dynamo_field] = value
            else:
                logger.debug(f"Could not find CSV field '{csv_field}' or value is None")
    
    return result


def transform_row(row: Dict[str, str], config: Config) -> Dict[str, Any]:
    """Transform a CSV row into a DynamoDB item using schema mapping."""
    # If no schema is defined, use the default mapping
    if not config.schema or not config.schema.get('mapping'):
        return {
            config.hash_key or 'myHashKey': row.get('column_a', next(iter(row.values()), '')),
            config.range_key or 'myRangeKey': row.get('column_b', ''),
            'myAttributes': {
                'attributeA': row.get('column_c', ''),
                'attributeB': row.get('column_d', '')
            }
        }
    
    # Use schema mapping
    schema_mapping = config.schema.get('mapping', {})
    return build_nested_structure(row, schema_mapping)


def write_to_dynamo(table, rows: Iterator[Dict[str, str]], config: Config, tracker: Optional[ProgressTracker] = None) -> Tuple[int, int]:
    """Write rows to DynamoDB using concurrent batch processing."""
    successful = 0
    failed = 0
    
    # Start progress tracking if available
    if tracker:
        tracker.start()
    
    # Create batches
    batches = []
    current_batch = []
    
    for row in rows:
        try:
            # Transform row to DynamoDB format
            item = transform_row(row, config)
            if item:  # Only add if transformation was successful
                current_batch.append(item)
                
                if len(current_batch) >= config.batch_size:
                    batches.append(current_batch)
                    current_batch = []
        except Exception as e:
            logger.error(f"Error processing row: {e}")
            failed += 1
            if tracker:
                tracker.update(0, 1)
    
    # Add the last batch if not empty
    if current_batch:
        batches.append(current_batch)
    
    logger.info(f"Created {len(batches)} batches from CSV data")
    
    # Process batches in parallel
    with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        futures = [executor.submit(process_batch, table, batch) for batch in batches]
        
        for future in futures:
            try:
                batch_successful, batch_failed = future.result()
                successful += batch_successful
                failed += batch_failed
                
                # Update progress tracker
                if tracker:
                    tracker.update(batch_successful, batch_failed)
                    
            except Exception as e:
                logger.error(f"Error processing batch: {e}")
                # Assume all items in the batch failed
                batch_size = min(config.batch_size, len(batches[0]) if batches else 0)
                failed += batch_size
                
                # Update progress tracker for failed batch
                if tracker:
                    tracker.update(0, batch_size)
    
    # Complete progress tracking
    if tracker:
        tracker.complete()
    
    return successful, failed


def validate_schema(config: Config) -> bool:
    """Validate schema structure and required fields."""
    schema = config.schema
    
    # Basic schema validation
    if not schema:
        return True  # No schema to validate
    
    # Check for required fields
    if 'mapping' not in schema:
        logger.error("Schema is missing required 'mapping' field")
        return False
    
    # Validate hash_key and range_key exist in schema
    if config.hash_key and config.hash_key not in schema.get('mapping', {}):
        logger.warning(f"Hash key '{config.hash_key}' is not directly defined in schema mapping")
    
    if config.range_key and config.range_key not in schema.get('mapping', {}):
        logger.warning(f"Range key '{config.range_key}' is not directly defined in schema mapping")
    
    return True


def main() -> int:
    """Main function to run the import process."""
    tracker = None
    
    try:
        config = parse_arguments()
        logger.info(f"Starting import to table '{config.table_name}' from {config.csv_file}")
        
        # Initialize progress tracking
        if not getattr(config, 'no_monitor', False):
            try:
                # Count total rows for progress tracking
                total_items = count_csv_rows(config.csv_file, encoding=config.encoding)
                logger.info(f"CSV file contains {total_items} rows (excluding header)")
                
                # Initialize progress tracker
                tracker = ProgressTracker(
                    table_name=config.table_name,
                    file_path=config.csv_file,
                    total_items=total_items,
                    job_id=getattr(config, 'job_id', None)
                )
                logger.info(f"Progress monitoring enabled. Job ID: {tracker.job_id}")
                logger.info(f"Monitor your import at: http://localhost:5000")
            except Exception as e:
                logger.warning(f"Failed to initialize progress tracking: {e}")
                tracker = None
        
        # Validate schema if provided
        if config.schema_file:
            # Load schema
            try:
                with open(config.schema_file, 'r') as f:
                    config.schema = json.load(f)
                    logger.info(f"Loaded schema from {config.schema_file}")
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in schema file: {e}")
                if tracker:
                    tracker.fail(f"Invalid JSON in schema file: {e}")
                return 1
            except Exception as e:
                logger.error(f"Error reading schema file: {e}")
                if tracker:
                    tracker.fail(f"Error reading schema file: {e}")
                return 1
            
            # Set hash and range keys from schema if not specified in args
            if not config.hash_key and config.schema.get('hash_key'):
                config.hash_key = config.schema.get('hash_key')
            
            if not config.range_key and config.schema.get('range_key'):
                config.range_key = config.schema.get('range_key')
            
            # Validate schema
            if not validate_schema(config):
                logger.error("Schema validation failed")
                if tracker:
                    tracker.fail("Schema validation failed")
                return 1
        
        dynamodb = get_dynamodb_resource(config)
        table = dynamodb.Table(config.table_name)
        
        # Verify table exists and is accessible
        try:
            table_info = table.meta.client.describe_table(TableName=config.table_name)
            logger.info(f"Connected to table: {config.table_name}")
            
            # Get actual key schema from table if not specified
            if not config.hash_key or not config.range_key:
                key_schema = table_info['Table']['KeySchema']
                for key in key_schema:
                    if key['KeyType'] == 'HASH' and not config.hash_key:
                        config.hash_key = key['AttributeName']
                        logger.info(f"Using hash key from table: {config.hash_key}")
                    elif key['KeyType'] == 'RANGE' and not config.range_key:
                        config.range_key = key['AttributeName']
                        logger.info(f"Using range key from table: {config.range_key}")
        
        except ClientError as e:
            logger.error(f"Error accessing table: {e}")
            if tracker:
                tracker.fail(f"Error accessing table: {str(e)}")
            return 1
        
        # Display configuration
        logger.info(f"Hash key: {config.hash_key}")
        if config.range_key:
            logger.info(f"Range key: {config.range_key}")
        
        rows = read_csv_data(config.csv_file, encoding=config.encoding)
        
        # Process a sample row to validate schema before full import
        try:
            sample_rows = list(read_csv_data(config.csv_file, encoding=config.encoding))
            if sample_rows:
                logger.info("Validating schema with sample row...")
                sample_item = transform_row(sample_rows[0], config)
                if not sample_item:
                    raise ValueError("Failed to transform sample row")
                
                # Validate hash key and range key
                if config.hash_key and config.hash_key not in sample_item:
                    raise ValueError(f"Hash key '{config.hash_key}' not found in transformed item")
                if config.range_key and config.range_key not in sample_item:
                    raise ValueError(f"Range key '{config.range_key}' not found in transformed item")
            else:
                logger.warning("No sample rows available for schema validation")
        except Exception as e:
            logger.error(f"Schema validation failed with sample row: {e}")
            if tracker:
                tracker.fail(f"Schema validation failed with sample row: {str(e)}")
            return 1
        
        successful, failed = write_to_dynamo(
            table, 
            rows, 
            config,
            tracker
        )
        
        logger.info(f"Import complete: {successful} items imported successfully, {failed} items failed")
        
        if failed > 0 and successful == 0:
            logger.error("All items failed to import")
            return 1
        elif failed > 0:
            logger.warning(f"{failed} items failed to import")
            return 0
        else:
            return 0
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        if tracker:
            tracker.fail(f"Unhandled exception: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
