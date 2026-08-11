# Changelog

## [0.3.0](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/compare/v0.2.0...v0.3.0) (2026-08-04)


### Features

* add CODEOWNERS file to maintain dependabot PRs ([#29](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/issues/29)) ([53b5483](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/commit/53b548393a3173a516de3f7d9a9514f7c488d304))
* migrate to Lambda + Step functions workflow ([#5](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/issues/5)) ([314fa11](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/commit/314fa11dcd791d350e3bdb61435f52889ae59f85))
* publish each collection to an iceberg table ([#36](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/issues/36)) ([7a5b1d6](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/commit/7a5b1d6e43bd305b89b9148367fae047549d23b4))
* scrape records every 5 days ([#9](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/issues/9)) ([9d131c0](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/commit/9d131c0d166ca994ef51e0cb453190640599a319))
* use rustac.geoparquet_writer to stream items to parquet ([#4](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/issues/4)) ([f20bb82](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/commit/f20bb82000ebdffa290dd21f84c0577c95eba0ad))


### Bug Fixes

* add datetime metrics to iceberg ([#37](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/issues/37)) ([526a65e](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/commit/526a65ef7deeb3f9dfbf417a73eb144a59424953))
* change cron schedule ([51c2426](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/commit/51c242690572b74122d90ed3b4c2f4b79681e88f))
* handle yearmonth arg for manual invocations ([82afdfe](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/commit/82afdfe73410ad195c0c6f7b7478ada269b1ffe1))
* include existing files in iceberg metadata ([#41](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/issues/41)) ([d6d92a7](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/commit/d6d92a7e2809a3adcc6ecbfab7c89e48d5cccece))
* maintain Hilbert sort order throughout write process ([#12](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/issues/12)) ([f9f2fac](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/commit/f9f2faca0ec9675eed312f70def952fcc284a722))
* unify parquet schemas before writing iceberg ([66616e9](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/commit/66616e9558b0d9d965546907c2d6455df7ae6e3a))


### Documentation

* add docs site ([#6](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/issues/6)) ([cda009c](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/commit/cda009c345dff27c2e098861139bdea498b568a0))
* remove manual cache-daily guide from README ([ced9ecd](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/commit/ced9ecd60c8f57d6285be05c80128fa06afb56e6))
* show how to run all write-monthly jobs for historical archive ([02f1c4f](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/commit/02f1c4ffdb8f311ca47a8cdb4b73a726e07c669e))
* update chart in docs page for December 2025 ([87c9a38](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/commit/87c9a38d604aa1b198c0a33aec541bddba57f4db))
* update README and docs page ([912cf3d](https://github.com/MAAP-Project/hls-stac-geoparquet-archive/commit/912cf3de7e2d677d63d80580ce1cc1aeccd8ba4d))

## 0.2.0 (2025-11-17)

### Features

- Add the command-line interface.
- Add AWS CDK deployment infrastructure.
- Split STAC link caching from GeoParquet writing.
