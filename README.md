# DynamoDB CSV Importer with Schema Mapping

This guide demonstrates how to use the enhanced DynamoDB CSV importer with schema mapping capabilities.

## Understanding Schema Mapping

The schema mapping allows you to define how columns in your CSV file should be transformed into DynamoDB attributes. The schema is defined in a JSON file and includes:

- **hash_key**: The primary key attribute name in DynamoDB
- **range_key**: (Optional) The sort key attribute name in DynamoDB
- **mapping**: Defines how CSV columns map to DynamoDB attributes

### Field Mappings

Each mapping is defined as:

```
"DynamoDBAttributeName": "csv_column_name:data_type"
```

Where `data_type` is one of the following:

- `S`: String
- `N`: Number (integer or float)
- `BOOL`: Boolean
- `B`: Binary
- `NULL`: Null value
- `M`: Map (nested attributes)
- `L:type`: List of items with specified subtype (e.g., `L:S` for list of strings)
- `SS`: String Set
- `NS`: Number Set
- `BS`: Binary Set

### Nested Attributes

Nested structures can be defined in two ways:

1. As a nested object:
   ```json
   "CustomerData": {
     "Name": "customer_name:S",
     "Email": "customer_email:S"
   }
   ```

2. As a map type:
   ```json
   "MetaData": {
     "type": "M",
     "fields": {
       "Created": "created_date:S",
       "Modified": "modified_date:S"
     }
   }
   ```

## AWS IAM Setup

Before using the script, you need to set up proper AWS IAM permissions:

### 1. Create an IAM Policy

Create a new policy named `DynamoDBImportPolicy` with the following content:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:BatchWriteItem",
                "dynamodb:PutItem",
                "dynamodb:DescribeTable"
            ],
            "Resource": "arn:aws:dynamodb:REGION:ACCOUNT_ID:table/*"
        }
    ]
}
```

Replace `REGION` and `ACCOUNT_ID` with your AWS region and account ID.

### 2. Create an IAM Role

Create a role named `DynamoDBImportRole` with the following trust policy:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::ACCOUNT_ID:root"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
```

Replace `ACCOUNT_ID` with your AWS account ID.

### 3. Attach the Policy to the Role

Attach the `DynamoDBImportPolicy` to the `DynamoDBImportRole`.

### 4. Configure AWS CLI Profile

Update your `~/.aws/config` file to include a profile for the role:

```
[profile dynamodb-import]
role_arn=arn:aws:iam::ACCOUNT_ID:role/DynamoDBImportRole
source_profile=default
```

Replace `ACCOUNT_ID` with your AWS account ID.

## Basic Usage

Run the script with the required parameters:

```bash
python dynamodb_csv_importer.py --table YourTableName --file your_data.csv --schema schema.json
```

## Example Usage Scenarios

### 1. Simple Import with Schema

```bash
python dynamodb_csv_importer.py --table ProductReviews --file reviews.csv --schema product_reviews_schema.json
```

### 2. Import with AWS Profile and Region

```bash
python dynamodb_csv_importer.py --table ProductReviews --file reviews.csv --schema product_reviews_schema.json --profile dynamodb-import --region eu-west-2
```

### 3. Import with Performance Tuning

For large datasets, adjust batch size and worker count:

```bash
python dynamodb_csv_importer.py --table ProductReviews --file reviews.csv --schema product_reviews_schema.json --batch-size 50 --workers 20
```

### 4. Import without Schema (Legacy Mode)

You can still use the script without a schema file by explicitly defining the hash and range keys:

```bash
python dynamodb_csv_importer.py --table ProductReviews --file reviews.csv --hash-key ProductId --range-key ReviewDate
```

## Sample Schema File

```json
{
  "hash_key": "ProductId",
  "range_key": "ReviewDate",
  "mapping": {
    "ProductId": "product_id:S",
    "ReviewDate": "review_date:S",
    "CustomerData": {
      "CustomerId": "customer_id:S",
      "Name": "customer_name:S",
      "Email": "customer_email:S"
    },
    "ReviewDetails": {
      "Rating": "rating:N",
      "Title": "review_title:S",
      "Content": "review_text:S"
    },
    "Verified": "verified:BOOL",
    "Tags": "tags:SS",
    "Metrics": {
      "Helpfulness": "helpfulness:N",
      "Upvotes": "upvotes:N",
      "Downvotes": "downvotes:N"
    },
    "Images": "image_urls:L:S",
    "UpdateHistory": {
      "type": "M",
      "fields": {
        "OriginalDate": "original_date:S",
        "UpdateCount": "update_count:N",
        "LastModified": "last_modified:S"
      }
    }
  }
}
```

## Common Issues and Troubleshooting

- **Missing Fields**: If a CSV row is missing a field required by the schema, that field will be omitted from the DynamoDB item.
- **Type Conversion Errors**: If a value cannot be converted to the specified type, the attribute will be omitted from the DynamoDB item.
- **Hash Key / Range Key Validation**: The script validates that all transformed items contain the required hash key and range key.
- **Rate Limiting**: If you encounter throttling due to DynamoDB provisioned throughput limits, try reducing the batch size and number of workers.

## Monitoring Import Progress

The script includes a web-based monitoring system to track import progress in real-time:

1. **Start the monitor server**:

```bash
python monitor_server.py
```

2. **View the dashboard** at http://localhost:5000

The dashboard shows:
- Current progress of all import jobs
- Success/failure statistics
- Estimated completion time
- Processing speed

## Handling Large Datasets (16M+ Records)

For very large datasets, use the included batch processing script:

### Windows (PowerShell)

```powershell
.\batch_import.ps1 -InputFile large_data.csv -TableName YourTable -SchemaFile schema.json -Region us-west-2 -Profile dynamodb-import
```

### Linux (Bash)

```bash
./batch_import.sh --input-file large_data.csv --table-name YourTable --schema-file schema.json --region us-west-2 --profile dynamodb-import
```

These scripts:
1. Split your large CSV file into manageable chunks
2. Process each chunk sequentially
3. Track progress and can resume from failures
4. Automatically start the monitoring server

### Batch Processing Parameters

| Parameter | Description | Default |
|-----------|-------------|--------|
| InputFile | Large CSV file to process | (Required) |
| TableName | DynamoDB table name | (Required) |
| SchemaFile | Schema mapping file | (Optional) |
| ChunkSize | Rows per chunk | 100,000 |
| BatchSize | Items per DynamoDB batch | 100 |
| Workers | Concurrent workers | 20 |
| Region | AWS region | (Optional) |
| Profile | AWS profile | (Optional) |
| NoMonitor | Disable web monitoring | False |

## Best Practices

1. **Validate your schema** with a small subset of your data before doing a full import.
2. **Monitor the dashboard** during import to track progress and identify issues.
3. **Test throughput settings** to find the optimal batch size and worker count for your specific table and data.
4. **Use appropriate data types** in your schema to ensure efficient storage and queries.
5. **Consider DynamoDB capacity** before starting a large import to avoid throttling.
6. **For multi-million record imports**, use the batch processing script with appropriate chunk sizes.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

The MIT License is a permissive license that is short and to the point. It lets people do almost anything they want with your project, like making and distributing closed source versions, as long as they include the original copyright and license notice in any copy of the software/source.

---

<div align="center">
  <p>
    <a href="https://www.windsurf.io" target="_blank">
      <img src="assets/windsurf-logo.svg" alt="Windsurf Logo" width="120" style="max-width: 100%;">
    </a>
  </p>
  <p>
    <b>Vibe coded with <a href="https://www.windsurf.io" target="_blank">Windsurf</a></b>
  </p>
</div>
