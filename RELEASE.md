# Release notes

## Unreleased

- Added `throttleLatestByKey(window)(key)` to rate-limit updates independently for each key and retain the latest value in each pending batch.
- Added `conflateByKey(window)(key)` to emit the latest value for each key seen in a shared window and flush the final batch when the stream completes.
- Added an sbt cross-build for Scala 2.13.18 and 3.10.1 with ZIO Streams 2.1.27. The shared tests cover empty input, independent throttles, and the final conflation batch.

See the [usage example](README.md#usage) for both extensions.
