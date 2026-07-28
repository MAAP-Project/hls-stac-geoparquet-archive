# HLS STAC Parquet

Query NASA's CMR for HLS (Harmonized Landsat Sentinel-2) satellite data and cache STAC items as GeoParquet files. Supports both local processing and AWS Lambda + Step Functions deployment.

The AWS Step Functions + Lambda pipeline writes a hive-partitioned parquet dataset following this pattern:
`s3://{bucket}/{prefix}/{version}/{collection}/year={year}/month={month}/*.parquet`.
It also publishes static Iceberg metadata at
`s3://{bucket}/{prefix}/{version}/{collection}/iceberg/metadata/latest.metadata.json` so DuckDB users can query without S3 `ListBucket` access.

## Development

```bash
git clone https://github.com/MAAP-project/hls-stac-parquet.git
cd hls-stac-parquet

uv sync
```

### Local MinIO pipeline check

Run a local S3-compatible archive and exercise cache links → write parquet → publish Iceberg → DuckDB read:

```bash
docker compose up -d minio minio-init
uv run python scripts/local_minio_pipeline.py
```

MinIO console: <http://localhost:9001> (`minioadmin` / `minioadmin`). The test bucket is `hls-local`; local compose grants anonymous `GetObject` access without anonymous bucket listing so the DuckDB Iceberg read uses public object URLs.

The script logs the plain DuckDB SQL needed for the local Iceberg metadata read. It uses a MinIO user with only `s3:GetObject`, not bucket listing:

```sql
INSTALL httpfs;
LOAD httpfs;
INSTALL iceberg;
LOAD iceberg;
SET s3_region='us-east-1';
SET s3_access_key_id='hlsreadonly';
SET s3_secret_access_key='hlsreadonly';
SET s3_endpoint='localhost:9000';
SET s3_url_style='path';
SET s3_use_ssl=false;
SELECT count(*) FROM iceberg_scan('s3://hls-local/archive/v2/HLSL30_2.0/iceberg/metadata/latest.metadata.json');
```

## CLI Usage

Two-step workflow for efficient data processing:

### 1. Cache Daily STAC Links

Query CMR and cache STAC JSON links for a specific day and collection:

```bash
uv run hls-stac-parquet cache-daily-stac-json-links HLSL30 2024-01-15 s3://bucket/data

# Optional: filter by bounding box (west, south, east, north)
uv run hls-stac-parquet cache-daily-stac-json-links HLSS30 2024-01-15 s3://bucket/data \
  --bounding-box -100,40,-90,50
```

### 2. Write Monthly GeoParquet

Read cached links and write monthly GeoParquet files. The deployed Lambda also publishes static Iceberg metadata after each monthly write.

```bash
uv run hls-stac-parquet write-monthly-stac-geoparquet HLSL30 2024-01-01 s3://bucket/links s3://bucket/data v2

# Optional: control validation
uv run hls-stac-parquet write-monthly-stac-geoparquet HLSS30 2024-01-01 s3://bucket/links s3://bucket/data v2 \
  --no-require-complete-links
```

Publish or repair Iceberg metadata for an existing monthly parquet file:

```bash
uv run hls-stac-parquet publish-static-iceberg-table HLSL30 2024 1 s3://bucket/data v2
```

### Collections

- `HLSL30` - [HLS Landsat Operational Land Imager Surface Reflectance and TOA Brightness Daily Global 30m v2.0](https://search.earthdata.nasa.gov/search/granules/collection-details?p=C2021957657-LPCLOUD&pg[0][v]=f&pg[0][gsk]=-start_date&q=hls)
- `HLSS30` - [HLS Sentinel-2 Multi-spectral Instrument Surface Reflectance Daily Global 30m v2.0](https://search.earthdata.nasa.gov/search/granules/collection-details?p=C2021957295-LPCLOUD&pg[0][v]=f&pg[0][gsk]=-start_date&q=hls)

### Output Structure

```
s3://bucket/data/
├── links/
│   ├── HLSL30.v2.0/2024/01/2024-01-01.json
│   ├── HLSL30.v2.0/2024/01/2024-01-02.json
    └── ...
└── v2/
    └── HLSL30_2.0/
        ├── year=2024/month=01/HLSL30_2.0-2024-1.parquet
        └── iceberg/metadata/latest.metadata.json
```

## AWS Deployment

Deploy scalable processing infrastructure with AWS CDK:

### Architecture

**Serverless Lambda + Step Functions:**
- **Cache Daily Lambda**: Lightweight CMR queries (1024 MB memory, 300s timeout, max 4 concurrent)
- **Write Monthly Lambda**: Write monthly GeoParquet files (8192 MB memory, 15min timeout, no concurrency limit)
- **Month Calculator Lambda**: Generate dates array for Step Functions (128 MB memory, 30s timeout)
- **Month List Generator Lambda**: Generate month list for backfill workflow (128 MB memory, 30s timeout)
- **Monthly Workflow State Machine**: Orchestrates single month processing (cache-daily → write-monthly)
- **Backfill Workflow State Machine**: Orchestrates multi-month historical backfill sequentially so static Iceberg metadata is not updated concurrently for a collection
- **EventBridge Rules**: Automated trigger every 5 days for both previous-month catch-up and current-month incremental updates (enabled by default)
- **CloudWatch Alarms**: Monitor Lambda errors and Step Functions timeouts, publishing to the SNS alert topic
- **Storage**: S3 bucket for cached STAC links
- **Logging**: CloudWatch logs for all Lambda functions and Step Functions executions

### Deployment

```bash
cd infrastructure
npm install && npm run build
npm run deploy
```

### Running Jobs

#### Automated Monthly Workflow (Step Functions)

The Step Functions state machine runs automatically every 5 days to keep the archive current. Each trigger fires four executions (staggered by one hour):

- 10:00 UTC — HLSL30 previous month (straggler catch-up)
- 11:00 UTC — HLSS30 previous month (straggler catch-up)
- 12:00 UTC — HLSL30 current month (incremental build)
- 13:00 UTC — HLSS30 current month (incremental build)

Each execution:
1. Calculates the target month's date range
2. Caches STAC links for all days in that month (parallel, max 4 concurrent)
3. Writes the monthly GeoParquet file
4. Publishes static Iceberg metadata for the collection

**Input Parameters:**

- **`collection`** (required): Either `"HLSL30"` or `"HLSS30"`
- **`yearmonth`** (optional): Specific month to process in format `"YYYY-MM-DD"` (day is ignored). If not provided, processes previous month

Note: `dest` and `version` are configured at deployment time and cannot be overridden at runtime.

**Manual Invocation:**

```bash
# Get the state machine ARN
STATE_MACHINE_ARN=$(aws cloudformation describe-stacks \
  --stack-name HlsStacGeoparquetArchive \
  --query 'Stacks[0].Outputs[?OutputKey==`MonthlyWorkflowStateMachineArn`].OutputValue' \
  --output text)

# Start execution - process previous month (default behavior)
aws stepfunctions start-execution \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --name "manual-hlsl30-$(date +%Y%m%d-%H%M%S)" \
  --input '{"collection": "HLSL30"}'

# Start execution - process a specific month
aws stepfunctions start-execution \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --name "manual-hlsl30-2024-11-$(date +%Y%m%d-%H%M%S)" \
  --input '{"collection": "HLSL30", "yearmonth": "2024-11-01"}'

# Monitor execution
EXECUTION_ARN=$(aws stepfunctions list-executions \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --max-results 1 \
  --query 'executions[0].executionArn' \
  --output text)

aws stepfunctions describe-execution --execution-arn "$EXECUTION_ARN"
```


#### Historical Backfills (Backfill Workflow)

> **WARNING:** The backfill workflow queries NASA's CMR API for each month in the requested range. Use responsibly for large historical backfills.

The backfill workflow is a parent Step Functions state machine that orchestrates the complete historical rebuild:

1. **Generate month list**: Calculate all year-months to process for a date range
2. **Process months sequentially**: Invoke the monthly workflow for each month, one at a time, so Iceberg table metadata is updated safely
3. **Each monthly workflow**: Cache all days (max 4 concurrent) → Write monthly GeoParquet → Publish Iceberg metadata

**Advantages over manual batch processing:**
- Infrastructure-managed, no long-running scripts
- Built-in sequential month processing to avoid concurrent Iceberg metadata writes
- Automatic retries and error handling
- Progress tracking in Step Functions console
- Can process entire collection history in one command

**Input Parameters:**

- **`collection`** (required): Either `"HLSL30"` or `"HLSS30"`
- **`start_date`** (optional): ISO format date (YYYY-MM-DD). Defaults to collection origin date (HLSL30: 2013-04-01, HLSS30: 2015-11-01)
- **`end_date`** (optional): ISO format date (YYYY-MM-DD). Defaults to last complete month

Note: `dest` and `version` are configured at deployment time and cannot be overridden at runtime.

**Running a Backfill:**

```bash
# Get the backfill state machine ARN
BACKFILL_STATE_MACHINE_ARN=$(aws cloudformation describe-stacks \
  --stack-name HlsStacGeoparquetArchive \
  --query 'Stacks[0].Outputs[?OutputKey==`BackfillStateMachineArn`].OutputValue' \
  --output text)

# Backfill entire HLSL30 history (2013-04 to present)
# WARNING: This will make ~50,000 CMR API requests over several hours
aws stepfunctions start-execution \
  --state-machine-arn "$BACKFILL_STATE_MACHINE_ARN" \
  --name "backfill-hlsl30-full-$(date +%Y%m%d-%H%M%S)" \
  --input '{"collection": "HLSL30"}'

# Backfill specific date range (2020-2024)
aws stepfunctions start-execution \
  --state-machine-arn "$BACKFILL_STATE_MACHINE_ARN" \
  --name "backfill-hlsl30-2020s-$(date +%Y%m%d-%H%M%S)" \
  --input '{
    "collection": "HLSL30",
    "start_date": "2020-01-01",
    "end_date": "2024-12-01"
  }'

# Monitor execution progress
EXECUTION_ARN=$(aws stepfunctions list-executions \
  --state-machine-arn "$BACKFILL_STATE_MACHINE_ARN" \
  --max-results 1 \
  --query 'executions[0].executionArn' \
  --output text)

aws stepfunctions describe-execution --execution-arn "$EXECUTION_ARN"

# View backfill workflow logs
aws logs tail /aws/vendedlogs/states/hls-backfill-workflow --follow
```

**Concurrency Settings:**
- **Backfill workflow**: Processes 1 month at a time because static Iceberg metadata publication is read-modify-write per collection
- **Monthly workflow**: Processes 4 days concurrently per month
- **Total concurrent CMR requests**: ~4 (1 month × 4 days)
- **write-monthly Lambda**: No concurrency limit (processes as many months as needed)

#### Manual Single-Month Processing

For ad-hoc processing of individual months, you can use either the monthly Step Functions workflow or invoke the write-monthly Lambda directly.

**Option 1: Via Step Functions (Recommended)**

Use this to process a single month including cache-daily and write-monthly:

```bash
# Process a specific month using Step Functions
STATE_MACHINE_ARN=$(aws cloudformation describe-stacks \
  --stack-name HlsStacGeoparquetArchive \
  --query 'Stacks[0].Outputs[?OutputKey==`MonthlyWorkflowStateMachineArn`].OutputValue' \
  --output text)

# This will cache all days in November 2024 and write the monthly file
aws stepfunctions start-execution \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --name "manual-2024-11-$(date +%Y%m%d-%H%M%S)" \
  --input '{"collection": "HLSL30", "yearmonth": "2024-11-01"}'
```

**Option 2: Direct write-monthly Lambda Invocation**

Use this only when cache-daily is already complete and you just need to write the parquet file and publish Iceberg metadata:

```bash
# Get the write-monthly Lambda function name
WRITE_MONTHLY_FUNCTION=$(aws cloudformation describe-stacks \
  --stack-name HlsStacGeoparquetArchive \
  --query 'Stacks[0].Outputs[?OutputKey==`WriteMonthlyFunctionName`].OutputValue' \
  --output text)

# Invoke for a specific month
# AWS CLI v2 requires --cli-binary-format raw-in-base64-out for inline JSON payloads
aws lambda invoke \
  --cli-binary-format raw-in-base64-out \
  --function-name "$WRITE_MONTHLY_FUNCTION" \
  --payload '{"collection": "HLSL30", "yearmonth": "2024-11-01"}' \
  response.json && cat response.json

# View logs
aws logs tail "/aws/lambda/$WRITE_MONTHLY_FUNCTION" --follow
```

**Available Parameters:**
- `collection`: "HLSL30" or "HLSS30" (required)
- `yearmonth`: Format "YYYY-MM-DD" (e.g., "2024-01-01") - day is ignored (required)
- `require_complete_links`: Optional. Boolean (default: true) - require all daily cache files before processing
- `skip_existing`: Optional. Boolean (default: true) - skip if output file already exists
- `batch_size`: Optional. Number of items per batch (default: 1000)

Note: `dest` and `version` are configured at deployment time via environment variables.

## Querying the Archive

Use the static Iceberg metadata path when you have object read access but not S3 bucket listing:

```sql
INSTALL iceberg;
LOAD iceberg;

CREATE OR REPLACE SECRET nasa_maap_data_store (
  TYPE S3,
  REGION 'us-west-2',
  PROVIDER credential_chain
);

SELECT *
FROM iceberg_scan('s3://nasa-maap-data-store/file-staging/nasa-map/hls-stac-geoparquet-archive/v2/HLSL30_2.0/iceberg/metadata/latest.metadata.json')
LIMIT 10;
```

Query `HLSL30_2.0` and `HLSS30_2.0` separately because their schemas differ. Users with `ListBucket` access can still query the hive-partitioned parquet files directly with `**/*.parquet` globs.


### Monitoring

#### Step Functions Workflow

View execution status and logs:

```bash
# Get state machine ARN
STATE_MACHINE_ARN=$(aws cloudformation describe-stacks \
  --stack-name HlsStacGeoparquetArchive \
  --query 'Stacks[0].Outputs[?OutputKey==`MonthlyWorkflowStateMachineArn`].OutputValue' \
  --output text)

# List recent executions
aws stepfunctions list-executions \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --max-results 10

# Get details of specific execution
aws stepfunctions describe-execution \
  --execution-arn <execution-arn>

# View execution history (see each step)
aws stepfunctions get-execution-history \
  --execution-arn <execution-arn> \
  --max-results 100

# View Step Functions logs
aws logs tail /aws/vendedlogs/states/hls-monthly-workflow --follow
```

#### Lambda Functions

View Lambda logs:

```bash
# Cache-daily Lambda
CACHE_DAILY_FUNCTION=$(aws cloudformation describe-stacks \
  --stack-name HlsStacGeoparquetArchive \
  --query 'Stacks[0].Outputs[?OutputKey==`LambdaFunctionName`].OutputValue' \
  --output text)
aws logs tail "/aws/lambda/$CACHE_DAILY_FUNCTION" --follow

# Write-monthly Lambda
WRITE_MONTHLY_FUNCTION=$(aws cloudformation describe-stacks \
  --stack-name HlsStacGeoparquetArchive \
  --query 'Stacks[0].Outputs[?OutputKey==`WriteMonthlyFunctionName`].OutputValue' \
  --output text)
aws logs tail "/aws/lambda/$WRITE_MONTHLY_FUNCTION" --follow

# Check for errors
aws logs filter-events \
  --log-group-name "/aws/lambda/$CACHE_DAILY_FUNCTION" \
  --filter-pattern "ERROR" \
  --start-time $(date -d '1 hour ago' +%s)000
```

#### SNS Notifications

The SNS alert topic receives two types of messages:

- **Step Functions notifications**: The monthly workflow publishes a success message (with collection, month, and record count) on completion, and a failure message (with error details) if the write-monthly step fails.
- **CloudWatch alarm notifications**: Alarms for cache-daily Lambda errors and Step Functions timeouts also publish to the same topic.

Subscribe your email to receive all notifications:

```bash
# Get alert topic ARN
ALERT_TOPIC_ARN=$(aws cloudformation describe-stacks \
  --stack-name HlsStacGeoparquetArchive \
  --query 'Stacks[0].Outputs[?OutputKey==`AlertTopicArn`].OutputValue' \
  --output text)

# Subscribe your email
aws sns subscribe \
  --topic-arn "$ALERT_TOPIC_ARN" \
  --protocol email \
  --notification-endpoint your-email@example.com

# View CloudWatch alarm status
aws cloudwatch describe-alarms --alarm-name-prefix HlsStacGeoparquetArchive
```

### Cleanup

```bash
cd infrastructure
npx cdk destroy
```


## Acknowledgments

- NASA's CMR API for providing access to HLS data
- The `rustac` library for efficient STAC GeoParquet writing
- The `obstore` library for high-performance object storage access
