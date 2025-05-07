#!/usr/bin/env python3
"""
Schema validation utility for DynamoDB CSV importer.
Validates a CSV file against a schema without importing data.
"""

import sys
import json
import logging
import argparse
from pathlib import Path
from typing import Dict, Any, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("schema_validator")

# Import functions from the main importer
try:
    from dynamodb_csv_importer import (
        read_csv_data, 
        transform_row, 
        Config
    )
except ImportError:
    logger.error("Could not import from dynamodb_csv_importer.py. Make sure it's in the same directory.")
    sys.exit(1)

def validate_schema(csv_file: Path, schema_file: Path, encoding: str = "utf-8-sig") -> bool:
    """
    Validate a CSV file against a schema without importing data.
    Returns True if validation passes, False otherwise.
    """
    try:
        # Load schema
        with open(schema_file, 'r') as f:
            schema = json.load(f)
            logger.info(f"Loaded schema from {schema_file}")
        
        # Create a config object
        config = Config(
            table_name="validation_only",  # Not used for actual import
            csv_file=csv_file,
            schema_file=schema_file,
            schema=schema,
            hash_key=schema.get("hash_key", ""),
            range_key=schema.get("range_key"),
            encoding=encoding
        )
        
        # Read a sample row
        sample_rows = list(read_csv_data(csv_file, encoding=encoding))
        if not sample_rows:
            logger.error("No data found in CSV file")
            return False
        
        # Try to transform a sample row
        logger.info("Validating schema with sample row...")
        sample_item = transform_row(sample_rows[0], config)
        if not sample_item:
            logger.error("Failed to transform sample row")
            return False
        
        # Validate hash key and range key
        if config.hash_key and config.hash_key not in sample_item:
            logger.error(f"Hash key '{config.hash_key}' not found in transformed item")
            return False
        
        if config.range_key and config.range_key not in sample_item:
            logger.error(f"Range key '{config.range_key}' not found in transformed item")
            return False
        
        # If we get here, validation passed
        logger.info("Schema validation successful!")
        logger.info(f"Sample transformed item: {json.dumps(sample_item, default=str)[:200]}...")
        return True
        
    except Exception as e:
        logger.error(f"Schema validation failed: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Validate a CSV file against a DynamoDB schema")
    parser.add_argument("--file", "-f", required=True, help="Path to CSV file")
    parser.add_argument("--schema", "-s", required=True, help="Path to schema file")
    parser.add_argument("--encoding", default="utf-8-sig", help="CSV file encoding")
    
    args = parser.parse_args()
    
    csv_file = Path(args.file)
    schema_file = Path(args.schema)
    
    # Validate paths
    if not csv_file.exists():
        logger.error(f"CSV file not found: {csv_file}")
        sys.exit(1)
    
    if not schema_file.exists():
        logger.error(f"Schema file not found: {schema_file}")
        sys.exit(1)
    
    # Run validation
    success = validate_schema(csv_file, schema_file, args.encoding)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
