# Release notes

## 0.1.0

- Added `throttleLatestByKey(window)(key)` to rate-limit updates independently for each key and retain the latest value in each pending batch.
- Added `conflateByKey(window)(key)` to emit the latest value for each key seen in a shared window and flush the final batch when the stream completes.
- Added `dailyCutoff` and `withinTimeWindow` under the separate `streamextension.timeGateSyntax._` import. Both use inclusive boundaries and support per-chunk or per-element checks. Windows can span midnight.
- Added `haltAtCutoff` to stop even an idle stream at today's deadline in the supplied zone. A skipped local cutoff expires at the daylight-saving jump, and a repeated cutoff expires on its first occurrence.
- Added an sbt cross-build for Scala 2.13.18 and 3.10.1 with ZIO Streams 2.1.27. The shared tests cover empty input, independent throttles, and the final conflation batch.

See the README examples for [keyed updates](README.md#keyed-updates) and [local-time gates](README.md#local-time-gates).
