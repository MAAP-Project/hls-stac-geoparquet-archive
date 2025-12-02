import {
  CfnOutput,
  Duration,
  RemovalPolicy,
  Size,
  StackProps,
  Stack,
  Tags,
  aws_batch as batch,
  aws_ec2 as ec2,
  aws_ecs as ecs,
  aws_iam as iam,
  aws_lambda as lambda,
  aws_lambda_event_sources as lambdaEventSources,
  aws_logs as logs,
  aws_s3 as s3,
  aws_sns as sns,
  aws_sns_subscriptions as snsSubscriptions,
  aws_sqs as sqs,
} from "aws-cdk-lib";
import { Construct } from "constructs";
import * as path from "path";

export interface HlsBatchStackProps extends StackProps {
  /**
   * VPC CIDR block for the batch environment
   * @default '10.0.0.0/16'
   */
  vpcCidr?: string;

  /**
   * Maximum vCPUs for the batch compute environment
   * @default 8
   */
  maxvCpus?: number;

  /**
   * Instance types to use for batch compute environment
   * @default ['m5', 'c5', 'r5']
   */
  instanceTypes?: ec2.InstanceType[];

  /**
   * S3 bucket name for storing parquet data
   * If not provided, a bucket will be created
   */
  bucketName?: string;
}

export class HlsBatchStack extends Stack {
  public readonly bucket: s3.Bucket;
  public readonly lambdaQueue: sqs.Queue;
  public readonly deadLetterQueue: sqs.Queue;
  public readonly snsTopic: sns.Topic;
  public readonly lambdaFunction: lambda.Function;
  public readonly batchPublisherFunction: lambda.Function;
  public readonly writeMonthlyJobQueue: batch.JobQueue;
  public readonly writeMonthlyJobDefinition: batch.EcsJobDefinition;

  constructor(scope: Construct, id: string, props?: HlsBatchStackProps) {
    super(scope, id, props);

    // Create VPC for Batch environment
    const vpc = new ec2.Vpc(this, "HlsBatchVpc", {
      ipAddresses: ec2.IpAddresses.cidr(props?.vpcCidr || "10.0.0.0/16"),
      maxAzs: 2,
      natGateways: 0,
      subnetConfiguration: [
        {
          cidrMask: 24,
          name: "public",
          subnetType: ec2.SubnetType.PUBLIC,
        },
        {
          cidrMask: 24,
          name: "private",
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
        },
      ],
    });

    // Add S3 VPC endpoint for private S3 access
    vpc.addGatewayEndpoint("S3Endpoint", {
      service: ec2.GatewayVpcEndpointAwsService.S3,
    });

    // Create S3 bucket for storing parquet data
    this.bucket = new s3.Bucket(this, "HlsParquetBucket", {
      bucketName: props?.bucketName,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      removalPolicy: RemovalPolicy.RETAIN,
    });

    // Create CloudWatch log group
    const logGroup = new logs.LogGroup(this, "HlsBatchLogGroup", {
      logGroupName: "/aws/batch/hls-stac-parquet",
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // Create IAM role for Batch job execution
    const jobExecutionRole = new iam.Role(this, "HlsBatchJobExecutionRole", {
      assumedBy: new iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AmazonECSTaskExecutionRolePolicy",
        ),
      ],
    });

    // Create IAM role for the Batch job (task role)
    const jobRole = new iam.Role(this, "HlsBatchJobRole", {
      assumedBy: new iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
    });

    this.bucket.grantReadWrite(jobRole);
    logGroup.grantWrite(jobRole);

    // Add STS assume role permission for HLS data access
    jobRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["sts:AssumeRole"],
        resources: ["*"],
      }),
    );

    // Create dead letter queue
    this.deadLetterQueue = new sqs.Queue(this, "DeadLetterQueue", {
      retentionPeriod: Duration.days(14),
    });

    // Create main queue
    this.lambdaQueue = new sqs.Queue(this, "Queue", {
      visibilityTimeout: Duration.seconds(300),
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      deadLetterQueue: {
        maxReceiveCount: 2,
        queue: this.deadLetterQueue,
      },
    });

    // Create SNS topic
    this.snsTopic = new sns.Topic(this, "Topic", {
      displayName: `${id}-StacLoaderTopic`,
    });

    // Subscribe the queue to the topic
    this.snsTopic.addSubscription(
      new snsSubscriptions.SqsSubscription(this.lambdaQueue),
    );

    // Create the lambda function
    const maxConcurrency = 4;
    const lambdaRuntime = lambda.Runtime.PYTHON_3_13;
    this.lambdaFunction = new lambda.Function(this, "Function", {
      runtime: lambdaRuntime,
      handler: "hls_stac_parquet.handler.handler",
      code: lambda.Code.fromDockerBuild(path.join(__dirname, "../../"), {
        file: "infrastructure/Dockerfile.lambda",
        platform: "linux/amd64",
        buildArgs: {
          PYTHON_VERSION: lambdaRuntime.toString().replace("python", ""),
        },
      }),
      memorySize: 1024,
      timeout: Duration.seconds(300),
      reservedConcurrentExecutions: maxConcurrency,
      logRetention: logs.RetentionDays.ONE_WEEK,
      environment: {
        BUCKET_NAME: this.bucket.bucketName,
      },
    });

    // Grant Lambda function permissions to read/write to S3 bucket
    this.bucket.grantReadWrite(this.lambdaFunction);

    // Add SQS event source to the lambda
    this.lambdaFunction.addEventSource(
      new lambdaEventSources.SqsEventSource(this.lambdaQueue, {
        batchSize: 20,
        maxBatchingWindow: Duration.minutes(1),
        maxConcurrency: maxConcurrency,
        reportBatchItemFailures: true,
      }),
    );

    // Create the batch publisher lambda function (lightweight, no custom dependencies)
    this.batchPublisherFunction = new lambda.Function(
      this,
      "BatchPublisherFunction",
      {
        runtime: lambdaRuntime,
        handler: "batch_publisher.handler",
        code: lambda.Code.fromAsset(path.join(__dirname, "../lambda")),
        memorySize: 512,
        timeout: Duration.minutes(15),
        logRetention: logs.RetentionDays.ONE_WEEK,
        environment: {
          TOPIC_ARN: this.snsTopic.topicArn,
          BUCKET_NAME: this.bucket.bucketName,
        },
      },
    );

    // Grant batch publisher permission to publish to SNS topic
    this.snsTopic.grantPublish(this.batchPublisherFunction);

    // Create Batch compute environment for write-monthly jobs (larger, memory-optimized)
    const writeMonthlyComputeEnvironment =
      new batch.ManagedEc2EcsComputeEnvironment(
        this,
        "HlsWriteMonthlyComputeEnv",
        {
          vpc,
          vpcSubnets: {
            subnetType: ec2.SubnetType.PUBLIC,
          },
          maxvCpus: props?.maxvCpus || 32,
          useOptimalInstanceClasses: true,
          allocationStrategy: batch.AllocationStrategy.BEST_FIT_PROGRESSIVE,
          spot: false,
        },
      );

    // Job queue for write-monthly jobs
    this.writeMonthlyJobQueue = new batch.JobQueue(
      this,
      "HlsWriteMonthlyJobQueue",
      {
        priority: 1,
        computeEnvironments: [
          {
            computeEnvironment: writeMonthlyComputeEnvironment,
            order: 1,
          },
        ],
      },
    );

    // Create Docker image asset (shared by both job definitions)
    const containerImage = ecs.ContainerImage.fromAsset(
      path.join(__dirname, "../../"),
      {
        file: "infrastructure/Dockerfile.ecr",
      },
    );

    // Job Definition 2: Write Monthly STAC GeoParquet
    this.writeMonthlyJobDefinition = new batch.EcsJobDefinition(
      this,
      "WriteMonthlyJobDefinition",
      {
        jobDefinitionName: "hls-write-monthly-stac-parquet",
        container: new batch.EcsEc2ContainerDefinition(
          this,
          "WriteMonthlyContainer",
          {
            image: containerImage,
            cpu: 2,
            memory: Size.mebibytes(8192),
            jobRole,
            executionRole: jobExecutionRole,
            logging: ecs.LogDriver.awsLogs({
              logGroup,
              streamPrefix: "hls-write-monthly",
            }),
            environment: {
              AWS_DEFAULT_REGION: this.region,
              EARTHDATA_USERNAME: process.env.EARTHDATA_USERNAME || "",
              EARTHDATA_PASSWORD: process.env.EARTHDATA_PASSWORD || "",
            },
            command: [
              "Ref::jobType",
              "Ref::collection",
              "Ref::yearMonth",
              "Ref::dest",
              "Ref::version",
              "Ref::requireCompleteLinks",
              "Ref::skipExisting",
            ],
          },
        ),
        parameters: {
          jobType: "write-monthly",
          collection: "",
          yearMonth: "",
          dest: `s3://${this.bucket.bucketName}`,
          version: "none",
          requireCompleteLinks: "true",
          skipExisting: "false",
        },
        retryAttempts: 3,
        timeout: Duration.minutes(60),
      },
    );

    // Create outputs
    new CfnOutput(this, "BucketName", {
      value: this.bucket.bucketName,
      description: "S3 bucket name for storing HLS parquet data",
    });

    new CfnOutput(this, "BucketArn", {
      value: this.bucket.bucketArn,
      description: "S3 bucket ARN for storing HLS parquet data",
    });

    new CfnOutput(this, "TopicArn", {
      value: this.snsTopic.topicArn,
      description: "SNS topic ARN for triggering cache-daily Lambda function",
    });

    new CfnOutput(this, "LambdaFunctionName", {
      value: this.lambdaFunction.functionName,
      description: "Lambda function name for cache-daily operations",
    });

    new CfnOutput(this, "BatchPublisherFunctionName", {
      value: this.batchPublisherFunction.functionName,
      description: "Lambda function name for batch publishing date ranges",
    });

    new CfnOutput(this, "BatchPublisherFunctionArn", {
      value: this.batchPublisherFunction.functionArn,
      description: "Lambda function ARN for batch publishing date ranges",
    });

    new CfnOutput(this, "QueueUrl", {
      value: this.lambdaQueue.queueUrl,
      description: "SQS queue URL for cache-daily Lambda function",
    });

    new CfnOutput(this, "DeadLetterQueueUrl", {
      value: this.deadLetterQueue.queueUrl,
      description: "Dead letter queue URL for failed cache-daily messages",
    });

    new CfnOutput(this, "WriteMonthlyJobQueueArn", {
      value: this.writeMonthlyJobQueue.jobQueueArn,
      description: "AWS Batch job queue ARN for write-monthly jobs",
    });

    new CfnOutput(this, "WriteMonthlyJobDefinitionArn", {
      value: this.writeMonthlyJobDefinition.jobDefinitionArn,
      description: "AWS Batch job definition ARN for writing monthly parquet",
    });

    // Output example job submission commands
    new CfnOutput(this, "ExampleBatchPublisherCommand", {
      value: [
        "aws lambda invoke",
        `--function-name ${this.batchPublisherFunction.functionName}`,
        "--payload '",
        JSON.stringify({
          collection: "HLSL30",
          start_date: "2024-01-01",
          end_date: "2024-01-31",
        }),
        "' response.json",
      ].join(" \\\n  "),
      description: "Example command to invoke batch publisher for a date range",
    });

    new CfnOutput(this, "ExampleWriteMonthlyCommand", {
      value: [
        "aws batch submit-job",
        '--job-name "hls-write-monthly-$(date +%Y%m%d-%H%M%S)"',
        `--job-queue ${this.writeMonthlyJobQueue.jobQueueName}`,
        `--job-definition ${this.writeMonthlyJobDefinition.jobDefinitionName}`,
        `--parameters 'collection=HLSL30,yearMonth=2024-01-01,dest=s3://${this.bucket.bucketName}/data'`,
      ].join(" \\\n  "),
      description: "Example command to submit a write-monthly batch job",
    });

    // Add tags
    Tags.of(this).add("Project", "HLS-STAC-Parquet");
    Tags.of(this).add("Environment", "Production");
  }
}
