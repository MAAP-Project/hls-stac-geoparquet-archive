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
  linkBucket: "hls-stac-geoparquet",
  destBucket: "nasa-maap-data-store",
  destPath: "file-staging/nasa-map/hls-stac-geoparquet-archive",
  dataVersion: "v2",
});
