# StreamExtension

StreamExtension 0.1.0 adds two independently importable sets of methods to ZIO's `ZStream`.
Use `streamextension.syntax._` to throttle or conflate keyed updates.
Use `streamextension.timeGateSyntax._` to filter or halt a stream by local time in a chosen zone.

## Keyed updates

Pass a duration in the first argument list and a function that extracts the key in the second.
This example uses a sensor ID as the key.

```scala
import streamextension.syntax._
import zio._
import zio.stream.ZStream

object SensorExample extends ZIOAppDefault {
  case class Reading(sensor: String, value: Int)
  val readings = ZStream(Reading("a", 1), Reading("b", 2), Reading("a", 3))

  val throttled = readings.throttleLatestByKey(1.second)(_.sensor)
  val conflated = readings.conflateByKey(1.hour)(_.sensor)

  def run = conflated.runForeach(reading => Console.printLine(reading))
}
```

`throttleLatestByKey` runs an independent throttle for each observed key.
While a throttle is busy, asynchronous aggregation keeps the latest reading
in its current batch. Each distinct key creates a substream, so use this
operator with a bounded set of keys on long-running streams.

`conflateByKey` emits the latest reading for each key when the window expires
or upstream completes. In the example, the source completes within the hour,
so `run` prints `Reading(a,3)` and `Reading(b,2)`. Order across keys is
unspecified for both operators.

## Local-time gates

```scala
import java.time.{LocalTime, ZoneId}
import streamextension.timeGateSyntax._
import zio.stream.ZStream

val zone = ZoneId.of("America/Toronto")
val cutoff = LocalTime.of(17, 0)
val events = ZStream(1, 2, 3)

val daytime = events.dailyCutoff(cutoff, zone)
val overnight = events.withinTimeWindow(LocalTime.of(22, 0), LocalTime.of(6, 0), zone, Granularity.PerElement)
val untilClose = events.haltAtCutoff(cutoff, zone)
```

`dailyCutoff` passes data through the cutoff instant and reopens at midnight.
`withinTimeWindow` includes both endpoints, wraps across midnight when the end
precedes the start, and matches a single instant when both endpoints are equal.
Both gates sample ZIO's clock once per chunk by default. Select
`Granularity.PerElement` to check each element. They log the initial state
and subsequent changes when data arrives.

`haltAtCutoff` ends the stream at today's deadline even while upstream is
idle. Starting at or after the deadline skips upstream. A cutoff in a
daylight-saving gap expires when the clock jumps forward, and a repeated
cutoff expires on its first occurrence.

## Build and test

With JDK 21 and sbt installed, run `sbt +test` from the project root to compile
and test with Scala 2.13.18 and 3.10.1. Both builds use ZIO Streams 2.1.27.

See the [release notes](RELEASE.md) for the feature history.

