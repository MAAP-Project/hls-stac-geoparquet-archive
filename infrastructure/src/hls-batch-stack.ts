import {
  CfnOutput,
  Duration,
  RemovalPolicy,
  Size,
  StackProps,
  Stack,
  Tags,
  aws_cloudwatch as cloudwatch,
  aws_cloudwatch_actions as cloudwatch_actions,
  aws_events as events,
  aws_events_targets as targets,
  aws_lambda as lambda,
  aws_lambda_event_sources as lambdaEventSources,
  aws_logs as logs,
  aws_s3 as s3,
  aws_sns as sns,
  aws_sns_subscriptions as snsSubscriptions,
  aws_sqs as sqs,
  aws_stepfunctions as sfn,
  aws_stepfunctions_tasks as tasks,
} from "aws-cdk-lib";
import { Construct } from "constructs";
import * as path from "path";

export interface HlsBatchStackProps extends StackProps {
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
  public readonly writeMonthlyFunction: lambda.Function;
  public readonly monthCalculatorFunction: lambda.Function;
  public readonly alertTopic: sns.Topic;
  public readonly monthlyWorkflowStateMachine: sfn.StateMachine;

  constructor(scope: Construct, id: string, props?: HlsBatchStackProps) {
    super(scope, id, props);

    // Create S3 bucket for storing parquet data
    this.bucket = new s3.Bucket(this, "HlsParquetBucket", {
      bucketName: props?.bucketName,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      removalPolicy: RemovalPolicy.RETAIN,
    });

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

    // Create the write-monthly Lambda function
    this.writeMonthlyFunction = new lambda.Function(
      this,
      "WriteMonthlyFunction",
      {
        runtime: lambdaRuntime,
        handler: "hls_stac_parquet.write_handler.handler",
        code: lambda.Code.fromDockerBuild(path.join(__dirname, "../../"), {
          file: "infrastructure/Dockerfile.lambda",
          platform: "linux/amd64",
          buildArgs: {
            PYTHON_VERSION: lambdaRuntime.toString().replace("python", ""),
          },
        }),
        memorySize: 8192,
        timeout: Duration.minutes(15),
        ephemeralStorageSize: Size.gibibytes(10),
        logRetention: logs.RetentionDays.ONE_WEEK,
        environment: {
          BUCKET_NAME: this.bucket.bucketName,
          EARTHDATA_USERNAME: process.env.EARTHDATA_USERNAME || "",
          EARTHDATA_PASSWORD: process.env.EARTHDATA_PASSWORD || "",
        },
      },
    );

    // Grant write-monthly Lambda permissions to read/write to S3 bucket
    this.bucket.grantReadWrite(this.writeMonthlyFunction);

    // Create the month calculator Lambda function (lightweight, no Docker build)
    this.monthCalculatorFunction = new lambda.Function(
      this,
      "MonthCalculatorFunction",
      {
        runtime: lambdaRuntime,
        handler: "month_calculator.handler",
        code: lambda.Code.fromAsset(path.join(__dirname, "../lambda")),
        memorySize: 128,
        timeout: Duration.seconds(30),
        logRetention: logs.RetentionDays.ONE_WEEK,
      },
    );

    // Create SNS topic for alerts
    this.alertTopic = new sns.Topic(this, "AlertTopic", {
      displayName: "HLS Monthly Workflow Alerts",
    });

    // Step Functions State Machine for Monthly Workflow

    // Step 1: Calculate previous month and generate dates array
    const calculatePreviousMonth = new tasks.LambdaInvoke(
      this,
      "CalculatePreviousMonth",
      {
        lambdaFunction: this.monthCalculatorFunction,
        outputPath: "$.Payload",
        comment: "Calculate previous month and generate array of dates",
      },
    );

    // Step 2: Map state to process all dates in parallel
    const cacheAllDays = new sfn.Map(this, "CacheAllDays", {
      itemsPath: "$.dates",
      maxConcurrency: 4, // Match cache-daily Lambda reserved concurrency
      resultPath: "$.cacheResults",
      comment: "Process cache-daily for each date in parallel",
    });

    // Cache-daily Lambda invocation for each date
    // Format the payload to match the existing SQS event structure
    const cacheDailyTask = new tasks.LambdaInvoke(this, "CacheDailyTask", {
      lambdaFunction: this.lambdaFunction,
      payload: sfn.TaskInput.fromObject({
        Records: [
          {
            "messageId.$": "$$.Execution.Id",
            body: {
              Message: {
                "date.$": "$.date",
                "collection.$": "$.collection",
                "dest.$": "$.dest",
                "skip_existing.$": "$.skip_existing",
              },
            },
          },
        ],
      }),
      resultPath: "$.result",
      comment: "Invoke cache-daily Lambda for a single date",
    });

    // Add retry logic for failed dates
    cacheDailyTask.addRetry({
      errors: ["States.TaskFailed", "Lambda.ServiceException"],
      interval: Duration.seconds(30),
      maxAttempts: 2,
      backoffRate: 2,
    });

    // Configure the Map iterator
    cacheAllDays.itemProcessor(cacheDailyTask);

    // Step 3: Write monthly parquet
    // Build payload dynamically to conditionally include version
    const writeMonthly = new tasks.LambdaInvoke(this, "WriteMonthly", {
      lambdaFunction: this.writeMonthlyFunction,
      payload: sfn.TaskInput.fromObject({
        "collection.$": "$.collection",
        "yearmonth.$": "$.yearMonth",
        "dest.$": "$.dest",
        "version.$": "$.version", // Pass through version if present
        require_complete_links: true,
        skip_existing: true,
      }),
      outputPath: "$.Payload",
      comment: "Write monthly GeoParquet file",
    });

    // Add retry logic to write-monthly step
    writeMonthly.addRetry({
      errors: ["States.TaskFailed", "States.Timeout"],
      interval: Duration.minutes(2),
      maxAttempts: 2,
      backoffRate: 1.5,
    });

    // Success state
    const success = new sfn.Succeed(this, "WorkflowSuccess", {
      comment: "Monthly workflow completed successfully",
    });

    // Failure state
    const failure = new sfn.Fail(this, "WorkflowFailure", {
      error: "MonthlyWorkflowFailed",
      cause: "Monthly workflow failed during execution",
    });

    // Add error handler to write-monthly step
    writeMonthly.addCatch(failure, {
      errors: ["States.ALL"],
      resultPath: "$.error",
    });

    // Chain the states together
    const definition = calculatePreviousMonth
      .next(cacheAllDays)
      .next(writeMonthly)
      .next(success);

    // Create state machine
    this.monthlyWorkflowStateMachine = new sfn.StateMachine(
      this,
      "MonthlyWorkflowStateMachine",
      {
        definitionBody: sfn.DefinitionBody.fromChainable(definition),
        timeout: Duration.hours(1),
        comment: "Orchestrates monthly cache-daily → write-monthly workflow",
        tracingEnabled: true,
        logs: {
          destination: new logs.LogGroup(this, "StateMachineLogGroup", {
            logGroupName: "/aws/vendedlogs/states/hls-monthly-workflow",
            retention: logs.RetentionDays.ONE_WEEK,
            removalPolicy: RemovalPolicy.DESTROY,
          }),
          level: sfn.LogLevel.ALL,
        },
      },
    );

    // EventBridge Rules for Monthly Trigger (15th of each month)
    // Rule for HLSL30 (10:00 AM UTC)
    const hlsl30MonthlyRule = new events.Rule(this, "MonthlyTriggerHLSL30", {
      schedule: events.Schedule.cron({
        minute: "0",
        hour: "10",
        day: "15",
        month: "*",
        year: "*",
      }),
      description:
        "Trigger monthly HLS workflow for HLSL30 on 15th of each month",
      enabled: true,
    });

    hlsl30MonthlyRule.addTarget(
      new targets.SfnStateMachine(this.monthlyWorkflowStateMachine, {
        input: events.RuleTargetInput.fromObject({
          collection: "HLSL30",
          dest: `s3://${this.bucket.bucketName}`,
          time: events.EventField.time,
        }),
      }),
    );

    // Rule for HLSS30 (11:00 AM UTC, 1 hour later to avoid overlap)
    const hlss30MonthlyRule = new events.Rule(this, "MonthlyTriggerHLSS30", {
      schedule: events.Schedule.cron({
        minute: "0",
        hour: "11",
        day: "15",
        month: "*",
        year: "*",
      }),
      description:
        "Trigger monthly HLS workflow for HLSS30 on 15th of each month",
      enabled: true,
    });

    hlss30MonthlyRule.addTarget(
      new targets.SfnStateMachine(this.monthlyWorkflowStateMachine, {
        input: events.RuleTargetInput.fromObject({
          collection: "HLSS30",
          dest: `s3://${this.bucket.bucketName}`,
          time: events.EventField.time,
        }),
      }),
    );

    // CloudWatch Alarms for monitoring

    // Alarm for write-monthly Lambda errors
    const writeMonthlyErrorAlarm = new cloudwatch.Alarm(
      this,
      "WriteMonthlyErrorAlarm",
      {
        metric: this.writeMonthlyFunction.metricErrors({
          period: Duration.minutes(5),
          statistic: "Sum",
        }),
        threshold: 1,
        evaluationPeriods: 1,
        alarmDescription: "Alert when write-monthly Lambda function fails",
        comparisonOperator:
          cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      },
    );
    writeMonthlyErrorAlarm.addAlarmAction(
      new cloudwatch_actions.SnsAction(this.alertTopic),
    );

    // Alarm for write-monthly Lambda success
    const writeMonthlySuccessAlarm = new cloudwatch.Alarm(
      this,
      "WriteMonthlySuccessAlarm",
      {
        metric: new cloudwatch.Metric({
          namespace: "AWS/Lambda",
          metricName: "Invocations",
          dimensionsMap: {
            FunctionName: this.writeMonthlyFunction.functionName,
          },
          period: Duration.minutes(5),
          statistic: "Sum",
        }),
        threshold: 1,
        evaluationPeriods: 1,
        alarmDescription:
          "Notify when write-monthly Lambda completes successfully",
        comparisonOperator:
          cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      },
    );
    writeMonthlySuccessAlarm.addAlarmAction(
      new cloudwatch_actions.SnsAction(this.alertTopic),
    );

    // Alarm for write-monthly Lambda throttles
    const writeMonthlyThrottleAlarm = new cloudwatch.Alarm(
      this,
      "WriteMonthlyThrottleAlarm",
      {
        metric: this.writeMonthlyFunction.metricThrottles({
          period: Duration.minutes(5),
          statistic: "Sum",
        }),
        threshold: 1,
        evaluationPeriods: 1,
        alarmDescription: "Alert when write-monthly Lambda is throttled",
        comparisonOperator:
          cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      },
    );
    writeMonthlyThrottleAlarm.addAlarmAction(
      new cloudwatch_actions.SnsAction(this.alertTopic),
    );

    // Alarm for cache-daily Lambda errors
    const cacheDailyErrorAlarm = new cloudwatch.Alarm(
      this,
      "CacheDailyErrorAlarm",
      {
        metric: this.lambdaFunction.metricErrors({
          period: Duration.minutes(5),
          statistic: "Sum",
        }),
        threshold: 5, // Allow a few failures, but alert if many dates fail
        evaluationPeriods: 1,
        alarmDescription: "Alert when cache-daily Lambda has multiple failures",
        comparisonOperator:
          cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      },
    );
    cacheDailyErrorAlarm.addAlarmAction(
      new cloudwatch_actions.SnsAction(this.alertTopic),
    );

    // Alarm for Step Functions execution failures
    const stateMachineFailureAlarm = new cloudwatch.Alarm(
      this,
      "StateMachineFailureAlarm",
      {
        metric: new cloudwatch.Metric({
          namespace: "AWS/States",
          metricName: "ExecutionsFailed",
          dimensionsMap: {
            StateMachineArn: this.monthlyWorkflowStateMachine.stateMachineArn,
          },
          period: Duration.minutes(5),
          statistic: "Sum",
        }),
        threshold: 1,
        evaluationPeriods: 1,
        alarmDescription: "Alert when Step Functions workflow fails",
        comparisonOperator:
          cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      },
    );
    stateMachineFailureAlarm.addAlarmAction(
      new cloudwatch_actions.SnsAction(this.alertTopic),
    );

    // Alarm for Step Functions execution timeouts
    const stateMachineTimeoutAlarm = new cloudwatch.Alarm(
      this,
      "StateMachineTimeoutAlarm",
      {
        metric: new cloudwatch.Metric({
          namespace: "AWS/States",
          metricName: "ExecutionsTimedOut",
          dimensionsMap: {
            StateMachineArn: this.monthlyWorkflowStateMachine.stateMachineArn,
          },
          period: Duration.minutes(5),
          statistic: "Sum",
        }),
        threshold: 1,
        evaluationPeriods: 1,
        alarmDescription: "Alert when Step Functions workflow times out",
        comparisonOperator:
          cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
      },
    );
    stateMachineTimeoutAlarm.addAlarmAction(
      new cloudwatch_actions.SnsAction(this.alertTopic),
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

    new CfnOutput(this, "WriteMonthlyFunctionName", {
      value: this.writeMonthlyFunction.functionName,
      description: "Lambda function name for write-monthly operations",
    });

    new CfnOutput(this, "WriteMonthlyFunctionArn", {
      value: this.writeMonthlyFunction.functionArn,
      description: "Lambda function ARN for write-monthly operations",
    });

    new CfnOutput(this, "MonthCalculatorFunctionName", {
      value: this.monthCalculatorFunction.functionName,
      description: "Lambda function name for month calculator",
    });

    new CfnOutput(this, "MonthlyWorkflowStateMachineArn", {
      value: this.monthlyWorkflowStateMachine.stateMachineArn,
      description: "Step Functions state machine ARN for monthly workflow",
    });

    new CfnOutput(this, "AlertTopicArn", {
      value: this.alertTopic.topicArn,
      description: "SNS topic ARN for workflow alerts",
    });

    new CfnOutput(this, "MonthlyRuleHLSL30", {
      value: hlsl30MonthlyRule.ruleName,
      description: "EventBridge rule name for HLSL30 monthly trigger",
    });

    new CfnOutput(this, "MonthlyRuleHLSS30", {
      value: hlss30MonthlyRule.ruleName,
      description: "EventBridge rule name for HLSS30 monthly trigger",
    });

    new CfnOutput(this, "QueueUrl", {
      value: this.lambdaQueue.queueUrl,
      description: "SQS queue URL for cache-daily Lambda function",
    });

    new CfnOutput(this, "DeadLetterQueueUrl", {
      value: this.deadLetterQueue.queueUrl,
      description: "Dead letter queue URL for failed cache-daily messages",
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

    new CfnOutput(this, "ExampleWriteMonthlyLambdaCommand", {
      value: [
        "aws lambda invoke",
        `--function-name ${this.writeMonthlyFunction.functionName}`,
        "--payload '",
        JSON.stringify({
          collection: "HLSL30",
          yearmonth: "2024-11-01",
        }),
        "' response.json && cat response.json",
      ].join(" \\\n  "),
      description:
        "Example command to invoke write-monthly Lambda function directly",
    });

    new CfnOutput(this, "ExampleStepFunctionsCommand", {
      value: [
        "# Process previous month (default):",
        "aws stepfunctions start-execution",
        `--state-machine-arn ${this.monthlyWorkflowStateMachine.stateMachineArn}`,
        '--name "test-$(date +%Y%m%d-%H%M%S)"',
        "--input '",
        JSON.stringify({
          collection: "HLSL30",
          dest: `s3://${this.bucket.bucketName}`,
        }),
        "'",
        "",
        "# Process specific month with version:",
        "aws stepfunctions start-execution",
        `--state-machine-arn ${this.monthlyWorkflowStateMachine.stateMachineArn}`,
        '--name "test-2024-11-$(date +%Y%m%d-%H%M%S)"',
        "--input '",
        JSON.stringify({
          collection: "HLSL30",
          dest: `s3://${this.bucket.bucketName}`,
          yearmonth: "2024-11-01",
          version: "v0.1.0",
        }),
        "'",
      ].join(" \\\n  "),
      description: "Example commands to test Step Functions workflow",
    });

    // Add tags
    Tags.of(this).add("Project", "HLS-STAC-Parquet");
    Tags.of(this).add("Environment", "Production");
  }
}
