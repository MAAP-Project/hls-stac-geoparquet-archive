# HLS STAC Parquet

Query NASA's CMR for HLS (Harmonized Landsat Sentinel-2) satellite data and cache STAC items as GeoParquet files. Supports both local processing and AWS Batch deployment.

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
└── v0.1/
    └── HLSL30.v2.0/year=2024/month=01/HLSL30_2.0-2025-1.parquet
```

## AWS Deployment

Deploy scalable processing infrastructure with AWS CDK:

### Architecture

- **Cache Daily Jobs**: SNS + SQS + Lambda for lightweight CMR queries (1024 MB memory, 300s timeout, max 4 concurrent)
- **Write Monthly Jobs**: AWS Batch with memory-optimized instances (2 vCPU, 8 GB) for writing monthly STAC GeoParquet files
- **Storage**: S3 bucket with VPC endpoint for efficient data transfer
- **Logging**: CloudWatch logs at `/aws/batch/hls-stac-parquet` (Batch) and `/aws/lambda/HlsBatchStack-Function*` (Lambda)

### Deployment

```bash
cd infrastructure
npm install && npm run build
npm run deploy
```

### Running Jobs

#### Cache Daily STAC Links (SNS + Lambda)

Publish messages to SNS to trigger the Lambda function. Get the SNS topic ARN from CloudFormation outputs:

```bash
# Get the SNS topic ARN
SNS_TOPIC_ARN=$(aws cloudformation describe-stacks \
  --stack-name HlsBatchStack \
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

#### Batch Publishing for Date Ranges**

Use the batch publisher Lambda function to automatically publish messages for all dates in a range:

**Batch Publisher Parameters:**
- `collection`: Required. Either "HLSL30" or "HLSS30"
- `start_date`: Optional. ISO format date (YYYY-MM-DD). Defaults to collection origin date (HLSL30: 2013-04-11, HLSS30: 2015-11-28)
- `end_date`: Optional. ISO format date (YYYY-MM-DD). Defaults to yesterday
- `dest`: Optional. S3 path like "s3://bucket/path" (defaults to stack's S3 bucket)
- `bounding_box`: Optional. Array of [min_lon, min_lat, max_lon, max_lat]
- `protocol`: Optional. Either "s3" or "https" (default: "s3")
- `skip_existing`: Optional. Boolean (default: true)

**Message Format:**
- `collection`: Required. Either "HLSL30" or "HLSS30"
- `date`: Required. ISO format date (YYYY-MM-DD)
- `dest`: Optional. S3 path like "s3://bucket/path" (defaults to stack's S3 bucket)
- `bounding_box`: Optional. Array of [min_lon, min_lat, max_lon, max_lat]
- `protocol`: Optional. Either "s3" or "https" (default: "s3")
- `skip_existing`: Optional. Boolean (default: true)

```bash
# Get the batch publisher function name
BATCH_PUBLISHER_FUNCTION=$(aws cloudformation describe-stacks \
  --stack-name HlsBatchStack \
  --query 'Stacks[0].Outputs[?OutputKey==`BatchPublisherFunctionName`].OutputValue' \
  --output text)

# Publish for all dates in a specific month
payload=`echo '{ "collection": "HLSL30", "start_date": "2025-10-01", "end_date": "2025-10-31" }' | openssl base64`
aws lambda invoke \
  --function-name "$BATCH_PUBLISHER_FUNCTION" \
  --payload "$payload" \
  /tmp/response.json

# Publish all available data from collection origin to yesterday
# (start_date defaults to collection origin, end_date defaults to yesterday)
payload=`echo '{ "collection": "HLSS30", "end_date": "2025-10-31" }' | openssl base64`
aws lambda invoke \
  --function-name "$BATCH_PUBLISHER_FUNCTION" \
  --payload "${payload}" \
  /tmp/response.json

# view the logs
BATCH_FUNCTION_NAME=$(aws cloudformation describe-stacks \
  --stack-name HlsBatchStack \
  --query 'Stacks[0].Outputs[?OutputKey==`BatchPublisherFunctionName`].OutputValue' \
  --output text)

# View recent logs (last 10 minutes)
aws logs tail "/aws/lambda/$BATCH_FUNCTION_NAME" --follow

```

#### Write Monthly GeoParquet Files (AWS Batch)

First, get the job queue, job definition, and bucket name from CloudFormation:

```bash
# Get job queue name
JOB_QUEUE=$(aws cloudformation describe-stacks \
  --stack-name HlsBatchStack \
  --query 'Stacks[0].Outputs[?OutputKey==`WriteMonthlyJobQueueArn`].OutputValue' \
  --output text | cut -d'/' -f2)

# Get job definition name
JOB_DEFINITION=$(aws cloudformation describe-stacks \
  --stack-name HlsBatchStack \
  --query 'Stacks[0].Outputs[?OutputKey==`WriteMonthlyJobDefinitionArn`].OutputValue' \
  --output text | cut -d'/' -f2 | cut -d':' -f1)

# Get bucket name for destination
BUCKET_NAME=$(aws cloudformation describe-stacks \
  --stack-name HlsBatchStack \
  --query 'Stacks[0].Outputs[?OutputKey==`BucketName`].OutputValue' \
  --output text)

DEST="s3://${BUCKET_NAME}"
```

Submit jobs with parameters:

```bash
# Submit a single write-monthly job
aws batch submit-job \
  --job-name "write-monthly-HLSS30-2025-10-$(date +%Y%m%d-%H%M%S)" \
  --job-queue "$JOB_QUEUE" \
  --job-definition "$JOB_DEFINITION" \
  --parameters "jobType=write-monthly,collection=HLSS30,yearMonth=2025-10-01,dest=$DEST,requireCompleteLinks=true,skipExisting=true"

# Function to submit jobs for a date range
submit_jobs_for_range() {
    local collection=$1
    local start_year_month=$2  # Format: YYYY-MM
    local end_year_month=$3    # Format: YYYY-MM

    # Convert to first day of month for date arithmetic
    current_date="${start_year_month}-01"
    end_date="${end_year_month}-01"

    while [[ "$current_date" < "$end_date" ]] || [[ "$current_date" == "$end_date" ]]; do
        year_month=$(date -d "$current_date" +%Y-%m-01)

        echo "Submitting job for $collection $year_month"
        aws batch submit-job \
          --job-name "write-monthly-${collection}-${year_month}-$(date +%Y%m%d-%H%M%S)" \
          --job-queue "$JOB_QUEUE" \
          --job-definition "$JOB_DEFINITION" \
          --parameters "jobType=write-monthly,collection=${collection},yearMonth=${year_month},dest=$DEST,requireCompleteLinks=true,skipExisting=true"

        # Move to next month
        current_date=$(date -d "$current_date + 1 month" +%Y-%m-01)
    done
}

# Submit for entire collection history
submit_jobs_for_range "HLSL30" "2013-04" "2025-10"
submit_jobs_for_range "HLSS30" "2015-11" "2025-10"

# Or submit for specific ranges
submit_jobs_for_range "HLSL30" "2024-01" "2024-12"
```

**Available Parameters:**
- `jobType`: Always "write-monthly"
- `collection`: "HLSL30" or "HLSS30"
- `yearMonth`: Format "YYYY-MM" (e.g., "2024-01")
- `dest`: S3 destination path (e.g., "s3://bucket-name")
- `requireCompleteLinks`: "true" or "false" (default: "true") - require all daily cache files before processing
- `skipExisting`: "true" or "false" (default: "true") - skip if output file already exists
- `version`: Version string for output path (e.g., "v0.1.0") or "none" use the deployed version


### Monitoring

#### Lambda Function (Cache Daily)

View recent logs:

```bash
# Get the Lambda function name
LAMBDA_FUNCTION_NAME=$(aws cloudformation describe-stacks \
  --stack-name HlsBatchStack \
  --query 'Stacks[0].Outputs[?OutputKey==`LambdaFunctionName`].OutputValue' \
  --output text)

# View recent logs (last 10 minutes)
aws logs tail "/aws/lambda/${LAMBDA_FUNCTION_NAME}" --follow

# Check for errors in the last hour
aws logs filter-events \
  --log-group-name "/aws/lambda/$LAMBDA_FUNCTION_NAME" \
  --filter-pattern "ERROR" \
  --start-time $(date -d '1 hour ago' +%s)000
```

Check SQS queue depth:

```bash
# Get queue URLs
QUEUE_URL=$(aws cloudformation describe-stacks \
  --stack-name HlsBatchStack \
  --query 'Stacks[0].Outputs[?OutputKey==`QueueUrl`].OutputValue' \
  --output text)

DLQ_URL=$(aws cloudformation describe-stacks \
  --stack-name HlsBatchStack \
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

#### Batch Jobs (Write Monthly)

```bash
# List recent jobs
aws batch list-jobs \
  --job-queue HlsBatchStack-HlsWriteMonthlyJobQueue \
  --job-status RUNNING

# View job logs
aws logs tail /aws/batch/hls-stac-parquet --follow
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
