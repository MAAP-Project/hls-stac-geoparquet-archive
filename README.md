# HLS STAC Parquet

Query NASA's CMR for HLS (Harmonized Landsat Sentinel-2) satellite data and cache STAC items as GeoParquet files. Supports both local processing and AWS Lambda + Step Functions deployment.

The AWS Step Functions + Lambda pipeline writes a hive-partitioned parquet dataset following this pattern:
`s3://{bucket}/{prefix}/{version}/{collection}/year={year}/month={month}/*.parquet`.

## Development

```bash
git clone https://github.com/MAAP-project/hls-stac-parquet.git
cd hls-stac-parquet

uv sync
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

Read cached links and write monthly GeoParquet files:

```bash
uv run hls-stac-parquet write-monthly-stac-geoparquet HLSL30 2024-01 s3://bucket/data

# Optional: version output and control validation
uv run hls-stac-parquet write-monthly-stac-geoparquet HLSS30 2024-01 s3://bucket/data \
  --version v0.1.0 \
  --no-require-complete-links
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
    └── HLSL30.v2.0/year=2024/month=01/HLSL30_2.0-2024-1.parquet
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
- **Backfill Workflow State Machine**: Orchestrates multi-month historical backfill (max 3 months in parallel)
- **EventBridge Rules**: Automated monthly trigger on 15th of each month (disabled by default)
- **SNS + SQS + DLQ**: Message queue for cache-daily operations with dead letter queue for failures
- **CloudWatch Alarms**: Monitor Lambda errors, throttles, and Step Functions failures
- **Storage**: S3 bucket for parquet data and cached STAC links
- **Logging**: CloudWatch logs for all Lambda functions and Step Functions executions

### Deployment

```bash
cd infrastructure
npm install && npm run build
npm run deploy
```

### Running Jobs

#### Automated Monthly Workflow (Step Functions)

The Step Functions state machine automatically runs on the 15th of each month (when enabled) to:
1. Calculate the previous month's date range (or use explicitly specified month)
2. Cache STAC links for all days in that month (parallel, max 4 concurrent)
3. Write the monthly GeoParquet file

**Input Parameters:**

- **`collection`** (required): Either `"HLSL30"` or `"HLSS30"`
- **`dest`** (optional): S3 destination path (e.g., `"s3://bucket-name"`). Defaults to stack bucket
- **`yearmonth`** (optional): Specific month to process in format `"YYYY-MM-DD"` (day is ignored). If not provided, processes previous month
- **`version`** (optional): Version string for output path (e.g., `"v0.1.0"`). Defaults to deployed version

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
  --input '{"collection": "HLSL30", "dest": "s3://your-bucket"}'

# Start execution - process a specific month
aws stepfunctions start-execution \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --name "manual-hlsl30-2024-11-$(date +%Y%m%d-%H%M%S)" \
  --input '{"collection": "HLSL30", "dest": "s3://your-bucket", "yearmonth": "2024-11-01"}'

# Start execution - with custom version
aws stepfunctions start-execution \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --name "manual-hlsl30-v0.2.0-$(date +%Y%m%d-%H%M%S)" \
  --input '{"collection": "HLSL30", "dest": "s3://your-bucket", "yearmonth": "2024-11-01", "version": "v0.2.0"}'

# Monitor execution
EXECUTION_ARN=$(aws stepfunctions list-executions \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --max-results 1 \
  --query 'executions[0].executionArn' \
  --output text)

aws stepfunctions describe-execution --execution-arn "$EXECUTION_ARN"
```

**Enable Automated Monthly Runs:**

```bash
# Enable EventBridge rules (one per collection)
HLSL30_RULE=$(aws cloudformation describe-stacks \
  --stack-name HlsStacGeoparquetArchive \
  --query 'Stacks[0].Outputs[?OutputKey==`MonthlyRuleHLSL30`].OutputValue' \
  --output text)

HLSS30_RULE=$(aws cloudformation describe-stacks \
  --stack-name HlsStacGeoparquetArchive \
  --query 'Stacks[0].Outputs[?OutputKey==`MonthlyRuleHLSS30`].OutputValue' \
  --output text)

aws events enable-rule --name "$HLSL30_RULE"
aws events enable-rule --name "$HLSS30_RULE"
```

#### Manual Cache Daily STAC Links (SNS + Lambda)

For ad-hoc caching or testing:

Publish messages to SNS to trigger the Lambda function. Get the SNS topic ARN from CloudFormation outputs:

```bash
# Get the SNS topic ARN
SNS_TOPIC_ARN=$(aws cloudformation describe-stacks \
  --stack-name HlsStacGeoparquetArchive \
  --query 'Stacks[0].Outputs[?OutputKey==`TopicArn`].OutputValue' \
  --output text)

# Cache for a single date
aws sns publish \
  --topic-arn "$SNS_TOPIC_ARN" \
  --message '{
    "collection": "HLSL30",
    "date": "2024-01-15"
  }'

# Cache with optional parameters
aws sns publish \
  --topic-arn "$SNS_TOPIC_ARN" \
  --message '{
    "collection": "HLSS30",
    "date": "2024-01-15",
    "bounding_box": [-100, 40, -90, 50],
    "protocol": "s3",
    "skip_existing": true
  }'

# Cache all days in a month (bash script example)
for day in {01..31}; do
  aws sns publish \
    --topic-arn "$SNS_TOPIC_ARN" \
    --message "{\"collection\": \"HLSL30\", \"date\": \"2024-01-${day}\"}"
done
```

#### Historical Backfills (Backfill Workflow)

> **WARNING:** The backfill workflow will query NASA's CMR API very heavily. It processes multiple months in parallel, with each month making ~30 CMR requests. Use responsibly and consider rate limiting for large historical backfills.

The backfill workflow is a parent Step Functions state machine that orchestrates the complete historical rebuild:

1. **Generate month list**: Calculate all year-months to process for a date range
2. **Process months in parallel**: Invoke the monthly workflow for each month (max 3 concurrent)
3. **Each monthly workflow**: Cache all days (max 4 concurrent) → Write monthly GeoParquet

**Advantages over manual batch processing:**
- Infrastructure-managed, no long-running scripts
- Built-in concurrency control to protect upstream API
- Automatic retries and error handling
- Progress tracking in Step Functions console
- Can process entire collection history in one command

**Input Parameters:**

- **`collection`** (required): Either `"HLSL30"` or `"HLSS30"`
- **`dest`** (required): S3 destination path (e.g., `"s3://bucket-name"`)
- **`start_date`** (optional): ISO format date (YYYY-MM-DD). Defaults to collection origin date (HLSL30: 2013-04-01, HLSS30: 2015-11-01)
- **`end_date`** (optional): ISO format date (YYYY-MM-DD). Defaults to last complete month
- **`version`** (optional): Version string for output path (e.g., `"v0.1.0"`)

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
  --input '{"collection": "HLSL30", "dest": "s3://your-bucket"}'

# Backfill specific date range (2020-2024)
aws stepfunctions start-execution \
  --state-machine-arn "$BACKFILL_STATE_MACHINE_ARN" \
  --name "backfill-hlsl30-2020s-$(date +%Y%m%d-%H%M%S)" \
  --input '{
    "collection": "HLSL30",
    "dest": "s3://your-bucket",
    "start_date": "2020-01-01",
    "end_date": "2024-12-01",
    "version": "v0.1.0"
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
- **Backfill workflow**: Processes 3 months concurrently
- **Monthly workflow**: Processes 4 days concurrently per month
- **Total concurrent CMR requests**: ~12 (3 months × 4 days)
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
  --input '{"collection": "HLSL30", "dest": "s3://your-bucket", "yearmonth": "2024-11-01"}'
```

**Option 2: Direct write-monthly Lambda Invocation**

Use this only when cache-daily is already complete and you just need to write the parquet file:

```bash
# Get the write-monthly Lambda function name
WRITE_MONTHLY_FUNCTION=$(aws cloudformation describe-stacks \
  --stack-name HlsStacGeoparquetArchive \
  --query 'Stacks[0].Outputs[?OutputKey==`WriteMonthlyFunctionName`].OutputValue' \
  --output text)

# Invoke for a specific month
aws lambda invoke \
  --function-name "$WRITE_MONTHLY_FUNCTION" \
  --payload '{"collection": "HLSL30", "yearmonth": "2024-11-01"}' \
  response.json && cat response.json

# View logs
aws logs tail "/aws/lambda/$WRITE_MONTHLY_FUNCTION" --follow
```

**Available Parameters:**
- `collection`: "HLSL30" or "HLSS30"
- `yearmonth`: Format "YYYY-MM-DD" (e.g., "2024-01-01") - day is ignored
- `dest`: Optional. S3 destination path (e.g., "s3://bucket-name"), defaults to stack bucket
- `require_complete_links`: Optional. Boolean (default: true) - require all daily cache files before processing
- `skip_existing`: Optional. Boolean (default: true) - skip if output file already exists
- `version`: Optional. Version string for output path (e.g., "v0.1.0"), defaults to deployed version
- `batch_size`: Optional. Number of items per batch (default: 1000)


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

Check SQS queue depth:

```bash
# Get queue URLs
QUEUE_URL=$(aws cloudformation describe-stacks \
  --stack-name HlsStacGeoparquetArchive \
  --query 'Stacks[0].Outputs[?OutputKey==`QueueUrl`].OutputValue' \
  --output text)

DLQ_URL=$(aws cloudformation describe-stacks \
  --stack-name HlsStacGeoparquetArchive \
  --query 'Stacks[0].Outputs[?OutputKey==`DeadLetterQueueUrl`].OutputValue' \
  --output text)

# Check messages in main queue
aws sqs get-queue-attributes \
  --queue-url "$QUEUE_URL" \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible

# Check messages in dead letter queue (failed messages)
aws sqs get-queue-attributes \
  --queue-url "$DLQ_URL" \
  --attribute-names ApproximateNumberOfMessages
```

#### CloudWatch Alarms

Subscribe to alerts for failures:

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

# View alarm status
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
