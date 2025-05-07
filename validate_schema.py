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
        transform_row, 
        type_converter,
        Config
    )
except ImportError:
    logger.error("Could not import from dynamodb_csv_importer.py. Make sure it's in the same directory.")
    sys.exit(1)

def read_csv_sample(file_path: Path, num_rows: int = 5, encoding: str = "utf-8-sig") -> list:
    """Read only the header and a few sample rows from a CSV file."""
    import csv
    
    encodings_to_try = [encoding, "latin-1", "cp1252", "iso-8859-1"]
    last_error = None
    
    for enc in encodings_to_try:
        try:
            sample_rows = []
            with open(file_path, 'r', newline='', encoding=enc) as f:
                reader = csv.DictReader(f)
                
                if not reader.fieldnames:
                    logger.warning("CSV file has no headers")
                    return []
                
                logger.info(f"Successfully opened CSV with encoding: {enc}")
                logger.info(f"CSV headers: {', '.join(reader.fieldnames)}")
                
                # Read only the specified number of rows
                for i, row in enumerate(reader):
                    if i >= num_rows:
                        break
                    
                    # Create normalized version of row with clean keys
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
                    
                    sample_rows.append(normalized_row)
                    
                return sample_rows
                
        except UnicodeDecodeError as e:
            last_error = e
            logger.debug(f"Failed to open CSV with encoding {enc}: {e}")
            continue
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            raise
    
    # If we get here, all encodings failed
    logger.error(f"Error reading CSV file with all attempted encodings. Last error: {last_error}")
    raise last_error

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
        
        # Read just a few sample rows instead of the entire file
        logger.info("Reading sample rows for validation (this is fast)...")
        sample_rows = read_csv_sample(csv_file, num_rows=2, encoding=encoding)
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
        # We need to check if the hash key and range key are in the transformed item
        # But we need to account for the fact that they might have been renamed in the mapping
        hash_key_found = False
        range_key_found = False
        
        # First check if the original keys are in the item
        if config.hash_key and config.hash_key in sample_item:
            hash_key_found = True
        
        if config.range_key and config.range_key in sample_item:
            range_key_found = True
            
        # If not found directly, check if they were transformed via the mapping
        if not hash_key_found and config.hash_key and config.schema and 'mapping' in config.schema:
            # Get the transformed name from the mapping
            mapping = config.schema['mapping']
            if config.hash_key in mapping:
                transformed_hash_key = mapping[config.hash_key].split(':')[0]
                if transformed_hash_key in sample_item:
                    hash_key_found = True
                    logger.info(f"Hash key '{config.hash_key}' was transformed to '{transformed_hash_key}' and found in item")
        
        if not range_key_found and config.range_key and config.schema and 'mapping' in config.schema:
            # Get the transformed name from the mapping
            mapping = config.schema['mapping']
            if config.range_key in mapping:
                transformed_range_key = mapping[config.range_key].split(':')[0]
                if transformed_range_key in sample_item:
                    range_key_found = True
                    logger.info(f"Range key '{config.range_key}' was transformed to '{transformed_range_key}' and found in item")
        
        # Report errors if keys not found
        if config.hash_key and not hash_key_found:
            logger.error(f"Hash key '{config.hash_key}' not found in transformed item (neither original nor transformed name)")
            return False
        
        if config.range_key and not range_key_found:
            logger.error(f"Range key '{config.range_key}' not found in transformed item (neither original nor transformed name)")
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
