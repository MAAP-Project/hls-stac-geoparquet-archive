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
  aws_logs as logs,
  aws_s3 as s3,
  aws_sns as sns,
  aws_stepfunctions as sfn,
  aws_stepfunctions_tasks as tasks,
} from "aws-cdk-lib";
import { Construct } from "constructs";
import * as path from "path";

export interface HlsBatchStackProps extends StackProps {
  /**
   * S3 bucket name for storing STAC JSON links
   */
  linkBucket: string;

  /**
   * S3 bucket name for writing STAC Geoparquet files
   */
  destBucket: string;

  /**
   * S3 path prefix for writing STAC Geoparquet files (optional)
   * Example: "path/to/destination" (no leading or trailing slashes needed)
   */
  destPath?: string;

  /**
   * Version string for pipeline output files.
   */
  dataVersion: string;
}

export class HlsStacGeoparquetStack extends Stack {
  public readonly bucket: s3.IBucket;
  public readonly cacheDailyFunction: lambda.Function;
  public readonly writeMonthlyFunction: lambda.Function;
  public readonly monthCalculatorFunction: lambda.Function;
  public readonly monthListGeneratorFunction: lambda.Function;
  public readonly alertTopic: sns.Topic;
  public readonly monthlyWorkflowStateMachine: sfn.StateMachine;
  public readonly backfillStateMachine: sfn.StateMachine;

  constructor(scope: Construct, id: string, props: HlsBatchStackProps) {
    super(scope, id, props);

    this.bucket = s3.Bucket.fromBucketName(
      this,
      "HlsStacGeoparquetBucket",
      props?.linkBucket,
    );

    // Create the lambda function
    const maxConcurrency = 12;
    const lambdaRuntime = lambda.Runtime.PYTHON_3_13;
    this.cacheDailyFunction = new lambda.Function(this, "CacheDailyFunction", {
      runtime: lambdaRuntime,
      handler: "hls_stac_parquet.handler.handler",
      code: lambda.Code.fromDockerBuild(path.join(__dirname, "../../"), {
        file: "Dockerfile",
        platform: "linux/amd64",
        buildArgs: {
          PYTHON_VERSION: lambdaRuntime.toString().replace("python", ""),
        },
      }),
      memorySize: 1024,
      timeout: Duration.seconds(300),
      reservedConcurrentExecutions: maxConcurrency,
      logGroup: new logs.LogGroup(this, "CacheDailyLogGroup", {
        retention: logs.RetentionDays.ONE_WEEK,
        removalPolicy: RemovalPolicy.DESTROY,
      }),
      environment: {
        DEST: `s3://${this.bucket.bucketName}`,
      },
    });

    // Grant Lambda function permissions to read/write to S3 bucket
    this.bucket.grantReadWrite(this.cacheDailyFunction);

    // Create the write-monthly Lambda function
    // Construct destination S3 URI from bucket and optional path
    const destUri = props.destPath
      ? `s3://${props.destBucket}/${props.destPath}`
      : `s3://${props.destBucket}`;

    this.writeMonthlyFunction = new lambda.Function(
      this,
      "WriteMonthlyFunction",
      {
        runtime: lambdaRuntime,
        handler: "hls_stac_parquet.write_handler.handler",
        code: lambda.Code.fromDockerBuild(path.join(__dirname, "../../"), {
          file: "Dockerfile",
          platform: "linux/amd64",
          buildArgs: {
            PYTHON_VERSION: lambdaRuntime.toString().replace("python", ""),
          },
        }),
        memorySize: 8192,
        timeout: Duration.minutes(15),
        ephemeralStorageSize: Size.gibibytes(10),
        logGroup: new logs.LogGroup(this, "WriteMonthlyLogGroup", {
          retention: logs.RetentionDays.ONE_WEEK,
          removalPolicy: RemovalPolicy.DESTROY,
        }),
        environment: {
          SOURCE: `s3://${this.bucket.bucketName}`,
          DEST: destUri,
          EARTHDATA_USERNAME: process.env.EARTHDATA_USERNAME || "",
          EARTHDATA_PASSWORD: process.env.EARTHDATA_PASSWORD || "",
          VERSION: props.dataVersion,
        },
      },
    );

    // Grant write-monthly Lambda permissions to read/write to stack bucket
    this.bucket.grantReadWrite(this.writeMonthlyFunction);

    // Grant write permissions to destination bucket
    const destBucket = s3.Bucket.fromBucketName(
      this,
      "DestBucket",
      props.destBucket,
    );
    destBucket.grantReadWrite(this.writeMonthlyFunction);

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
        logGroup: new logs.LogGroup(this, "MonthCalculatorLogGroup", {
          retention: logs.RetentionDays.ONE_WEEK,
          removalPolicy: RemovalPolicy.DESTROY,
        }),
      },
    );

    // Create the month list generator Lambda function (lightweight, no Docker build)
    this.monthListGeneratorFunction = new lambda.Function(
      this,
      "MonthListGeneratorFunction",
      {
        runtime: lambdaRuntime,
        handler: "month_list_generator.handler",
        code: lambda.Code.fromAsset(path.join(__dirname, "../lambda")),
        memorySize: 128,
        timeout: Duration.seconds(30),
        logGroup: new logs.LogGroup(this, "MonthListGeneratorLogGroup", {
          retention: logs.RetentionDays.ONE_WEEK,
          removalPolicy: RemovalPolicy.DESTROY,
        }),
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
    // Note: STAC JSON links are always written to the stack's bucket (BUCKET_NAME env var)
    const cacheDailyTask = new tasks.LambdaInvoke(this, "CacheDailyTask", {
      lambdaFunction: this.cacheDailyFunction,
      payload: sfn.TaskInput.fromObject({
        "date.$": "$.date",
        "collection.$": "$.collection",
        "skip_existing.$": "$.skip_existing",
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
    // Note: STAC JSON links are read from SOURCE env var
    // GeoParquet files are written to DEST env var
    const writeMonthly = new tasks.LambdaInvoke(this, "WriteMonthly", {
      lambdaFunction: this.writeMonthlyFunction,
      payload: sfn.TaskInput.fromObject({
        "collection.$": "$.collection",
        "yearmonth.$": sfn.JsonPath.format(
          "{}-01",
          sfn.JsonPath.stringAt("$.yearMonth"),
        ),
        require_complete_links: true,
        "skip_existing.$": "$.skip_existing",
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

    // Notify success via SNS with collection, month, and record count
    // At this point outputPath: "$.Payload" has replaced state with the Lambda response,
    // so $.collection, $.yearmonth, and $.total_items_written are all available.
    const notifySuccess = new tasks.SnsPublish(this, "NotifySuccess", {
      topic: this.alertTopic,
      subject: sfn.JsonPath.format(
        "HLS STAC GeoParquet Archive updated: {} {}",
        sfn.JsonPath.stringAt("$.collection"),
        sfn.JsonPath.stringAt("$.yearmonth"),
      ),
      message: sfn.TaskInput.fromText(
        sfn.JsonPath.format(
          "The HLS STAC GeoParquet Archive for {} ({}) was updated and now contains {} records.",
          sfn.JsonPath.stringAt("$.yearmonth"),
          sfn.JsonPath.stringAt("$.collection"),
          sfn.JsonPath.stringAt("$.total_items_written"),
        ),
      ),
      resultPath: sfn.JsonPath.DISCARD,
    });

    // Notify failure via SNS with collection, month, and error details.
    // The catch fires on the input to WriteMonthly (before outputPath transforms it),
    // so $.collection and $.yearMonth are still available alongside $.error.
    const notifyFailure = new tasks.SnsPublish(this, "NotifyFailure", {
      topic: this.alertTopic,
      subject: sfn.JsonPath.format(
        "HLS STAC GeoParquet Archive FAILED: {} {}",
        sfn.JsonPath.stringAt("$.collection"),
        sfn.JsonPath.stringAt("$.yearMonth"),
      ),
      message: sfn.TaskInput.fromText(
        sfn.JsonPath.format(
          "The HLS STAC GeoParquet Archive workflow failed for {} {}.\n\nError: {}\n\nCause:\n{}",
          sfn.JsonPath.stringAt("$.collection"),
          sfn.JsonPath.stringAt("$.yearMonth"),
          sfn.JsonPath.stringAt("$.error.Error"),
          sfn.JsonPath.stringAt("$.error.Cause"),
        ),
      ),
      resultPath: sfn.JsonPath.DISCARD,
    });
    notifyFailure.next(failure);

    // Add error handler to write-monthly step — route through failure notification
    writeMonthly.addCatch(notifyFailure, {
      errors: ["States.ALL"],
      resultPath: "$.error",
    });

    // Chain the states together
    const definition = calculatePreviousMonth
      .next(cacheAllDays)
      .next(writeMonthly)
      .next(notifySuccess)
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
            logGroupName:
              "/aws/vendedlogs/states/hls-stac-geoparquet-monthly-workflow",
            retention: logs.RetentionDays.ONE_WEEK,
            removalPolicy: RemovalPolicy.DESTROY,
          }),
          level: sfn.LogLevel.ALL,
        },
      },
    );

    // Parent Backfill Workflow State Machine
    // This workflow processes multiple months in parallel with controlled concurrency
    // WARNING: This will generate many cache-daily Lambda invocations and query CMR API heavily

    // Step 1: Generate list of months to process
    const generateMonthList = new tasks.LambdaInvoke(
      this,
      "GenerateMonthList",
      {
        lambdaFunction: this.monthListGeneratorFunction,
        outputPath: "$.Payload",
        comment: "Generate array of year-months for backfill",
      },
    );

    // Step 2: Map state to process all months in parallel with controlled concurrency
    const processAllMonths = new sfn.Map(this, "ProcessAllMonths", {
      itemsPath: "$.months",
      maxConcurrency: 3, // Process 3 months at a time
      resultPath: "$.monthResults",
      comment: "Process monthly workflow for each month in parallel",
    });

    // Invoke the monthly workflow for each month
    const invokeMonthlyWorkflow = new tasks.StepFunctionsStartExecution(
      this,
      "InvokeMonthlyWorkflow",
      {
        stateMachine: this.monthlyWorkflowStateMachine,
        integrationPattern: sfn.IntegrationPattern.RUN_JOB, // Wait for completion
        input: sfn.TaskInput.fromObject({
          "collection.$": "$.collection",
          "yearmonth.$": "$.yearmonth",
        }),
        resultPath: "$.workflowResult",
        comment: "Invoke monthly workflow for a single month",
      },
    );

    // Add retry logic for failed months
    invokeMonthlyWorkflow.addRetry({
      errors: ["States.TaskFailed", "States.Timeout"],
      interval: Duration.minutes(5),
      maxAttempts: 2,
      backoffRate: 2,
    });

    // Configure the Map iterator
    processAllMonths.itemProcessor(invokeMonthlyWorkflow);

    // Success state for backfill
    const backfillSuccess = new sfn.Succeed(this, "BackfillSuccess", {
      comment: "Backfill workflow completed successfully",
    });

    // Chain the states together
    const backfillDefinition = generateMonthList
      .next(processAllMonths)
      .next(backfillSuccess);

    // Create backfill state machine
    this.backfillStateMachine = new sfn.StateMachine(
      this,
      "BackfillStateMachine",
      {
        definitionBody: sfn.DefinitionBody.fromChainable(backfillDefinition),
        timeout: Duration.hours(48), // Allow up to 48 hours for large backfills
        comment:
          "Orchestrates historical backfill by processing multiple months in parallel",
        tracingEnabled: true,
        logs: {
          destination: new logs.LogGroup(this, "BackfillStateMachineLogGroup", {
            logGroupName:
              "/aws/vendedlogs/states/hls-stac-geoparquet-backfill-workflow",
            retention: logs.RetentionDays.ONE_WEEK,
            removalPolicy: RemovalPolicy.DESTROY,
          }),
          level: sfn.LogLevel.ALL,
        },
      },
    );

    // EventBridge Rules — fire every 5 days (days 1, 6, 11, 16, 21, 26 of each month).
    // Each trigger day runs four executions (staggered by 1 hour each):
    //   10:00 UTC — HLSL30 previous month (straggler catch)
    //   11:00 UTC — HLSS30 previous month (straggler catch)
    //   12:00 UTC — HLSL30 current month (incremental build)
    //   13:00 UTC — HLSS30 current month (incremental build)

    // Previous-month rules (default month_offset=-1 in month_calculator)
    const hlsl30MonthlyRule = new events.Rule(this, "MonthlyTriggerHLSL30", {
      schedule: events.Schedule.cron({
        minute: "0",
        hour: "5",
        day: "*/5",
        month: "*",
        year: "*",
      }),
      description:
        "Trigger HLS workflow for HLSL30 previous month every 5 days",
      enabled: true,
    });

    hlsl30MonthlyRule.addTarget(
      new targets.SfnStateMachine(this.monthlyWorkflowStateMachine, {
        input: events.RuleTargetInput.fromObject({
          collection: "HLSL30",
          month_offset: -1,
          skip_existing: false,
          time: events.EventField.time,
        }),
      }),
    );

    const hlss30MonthlyRule = new events.Rule(this, "MonthlyTriggerHLSS30", {
      schedule: events.Schedule.cron({
        minute: "0",
        hour: "6",
        day: "*/5",
        month: "*",
        year: "*",
      }),
      description:
        "Trigger HLS workflow for HLSS30 previous month every 5 days",
      enabled: true,
    });

    hlss30MonthlyRule.addTarget(
      new targets.SfnStateMachine(this.monthlyWorkflowStateMachine, {
        input: events.RuleTargetInput.fromObject({
          collection: "HLSS30",
          month_offset: -1,
          skip_existing: false,
          time: events.EventField.time,
        }),
      }),
    );

    // Current-month rules (month_offset=0 in month_calculator)
    const hlsl30CurrentMonthRule = new events.Rule(
      this,
      "CurrentMonthTriggerHLSL30",
      {
        schedule: events.Schedule.cron({
          minute: "0",
          hour: "7",
          day: "*/5",
          month: "*",
          year: "*",
        }),
        description:
          "Trigger HLS workflow for HLSL30 current month every 5 days",
        enabled: true,
      },
    );

    hlsl30CurrentMonthRule.addTarget(
      new targets.SfnStateMachine(this.monthlyWorkflowStateMachine, {
        input: events.RuleTargetInput.fromObject({
          collection: "HLSL30",
          month_offset: 0,
          skip_existing: false,
          time: events.EventField.time,
        }),
      }),
    );

    const hlss30CurrentMonthRule = new events.Rule(
      this,
      "CurrentMonthTriggerHLSS30",
      {
        schedule: events.Schedule.cron({
          minute: "0",
          hour: "8",
          day: "*/5",
          month: "*",
          year: "*",
        }),
        description:
          "Trigger HLS workflow for HLSS30 current month every 5 days",
        enabled: true,
      },
    );

    hlss30CurrentMonthRule.addTarget(
      new targets.SfnStateMachine(this.monthlyWorkflowStateMachine, {
        input: events.RuleTargetInput.fromObject({
          collection: "HLSS30",
          month_offset: 0,
          skip_existing: false,
          time: events.EventField.time,
        }),
      }),
    );

    // CloudWatch Alarms for monitoring

    // Alarm for cache-daily Lambda errors
    const cacheDailyErrorAlarm = new cloudwatch.Alarm(
      this,
      "CacheDailyErrorAlarm",
      {
        metric: this.cacheDailyFunction.metricErrors({
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

    new CfnOutput(this, "LambdaFunctionName", {
      value: this.cacheDailyFunction.functionName,
      description: "Lambda function name for cache-daily operations",
    });

    new CfnOutput(this, "WriteMonthlyFunctionName", {
      value: this.writeMonthlyFunction.functionName,
      description: "Lambda function name for write-monthly operations",
    });

    new CfnOutput(this, "WriteMonthlyFunctionArn", {
      value: this.writeMonthlyFunction.functionArn,
      description: "Lambda function ARN for write-monthly operations",
    });

    new CfnOutput(this, "WriteMonthlyFunctionRoleArn", {
      value: this.writeMonthlyFunction.role!.roleArn,
      description:
        "Lambda execution role ARN - use this in cross-account bucket policies",
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

    // Output example job submission commands
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
        "# Process previous month with skip_existing=true (skip already processed data):",
        "aws stepfunctions start-execution",
        `--state-machine-arn ${this.monthlyWorkflowStateMachine.stateMachineArn}`,
        '--name "test-$(date +%Y%m%d-%H%M%S)"',
        "--input '",
        JSON.stringify({
          collection: "HLSL30",
          skip_existing: true,
        }),
        "'",
        "",
        "# Process specific month with skip_existing=false (reprocess all data):",
        "aws stepfunctions start-execution",
        `--state-machine-arn ${this.monthlyWorkflowStateMachine.stateMachineArn}`,
        '--name "test-2024-11-$(date +%Y%m%d-%H%M%S)"',
        "--input '",
        JSON.stringify({
          collection: "HLSL30",
          yearmonth: "2024-11-01",
          skip_existing: false,
        }),
        "'",
      ].join(" \\\n  "),
      description: "Example commands to test Step Functions workflow",
    });

    new CfnOutput(this, "BackfillStateMachineArn", {
      value: this.backfillStateMachine.stateMachineArn,
      description: "Step Functions state machine ARN for backfill workflow",
    });

    new CfnOutput(this, "MonthListGeneratorFunctionName", {
      value: this.monthListGeneratorFunction.functionName,
      description: "Lambda function name for month list generator",
    });

    new CfnOutput(this, "ExampleBackfillCommand", {
      value: [
        "# WARNING: This will query CMR API heavily!",
        "# Backfill all HLSL30 history (2013-present):",
        "aws stepfunctions start-execution",
        `--state-machine-arn ${this.backfillStateMachine.stateMachineArn}`,
        '--name "backfill-hlsl30-$(date +%Y%m%d-%H%M%S)"',
        "--input '",
        JSON.stringify({
          collection: "HLSL30",
        }),
        "'",
        "",
        "# Backfill specific date range:",
        "aws stepfunctions start-execution",
        `--state-machine-arn ${this.backfillStateMachine.stateMachineArn}`,
        '--name "backfill-hlsl30-2020s-$(date +%Y%m%d-%H%M%S)"',
        "--input '",
        JSON.stringify({
          collection: "HLSL30",
          start_date: "2020-01-01",
          end_date: "2024-12-01",
        }),
        "'",
      ].join(" \\\n  "),
      description:
        "Example commands to run backfill workflow (processes multiple months in parallel)",
    });

    new CfnOutput(this, "CrossAccountBucketPolicyExample", {
      value: JSON.stringify(
        {
          Version: "2012-10-17",
          Statement: [
            {
              Sid: "AllowHLSStackWriteAccess",
              Effect: "Allow",
              Principal: {
                AWS: this.writeMonthlyFunction.role!.roleArn,
              },
              Action: ["s3:PutObject", "s3:PutObjectAcl", "s3:GetObject"],
              Resource: "arn:aws:s3:::YOUR-CROSS-ACCOUNT-BUCKET/*",
            },
          ],
        },
        null,
        2,
      ),
      description:
        "Example bucket policy to add to cross-account destination buckets",
    });

    // Add tags
    Tags.of(this).add("Project", "HLS-STAC-Parquet");
    Tags.of(this).add("Environment", "Production");
  }
}
