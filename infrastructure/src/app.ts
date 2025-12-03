#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { HlsStacGeoparquetStack } from "./hls-stac-geoparquet-stack";

const app = new cdk.App();

new HlsStacGeoparquetStack(app, "HlsStacGeoparquetArchive", {
  description: "HLS STAC Geoparquet Archive",
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || "us-west-2",
  },
  bucketName: "hls-stac-geoparquet",
  allowedDestinationBuckets: ["hrodmn-scratch", "nasa-maap-data-store"],
  dataVersion: "v2",
});
