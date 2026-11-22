# StreamExtension

StreamExtension adds two methods to ZIO's `ZStream` for processing updates
by key. Use `throttleLatestByKey` to give each key its own emission rate.
Use `conflateByKey` to collect the latest updates across keys in a shared window.

## Usage

Import `streamextension.syntax._` to enable both methods. Pass a duration
in the first argument list and a function that extracts the key in the second.
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

## Build and test

With JDK 21 and sbt installed, run `sbt +test` from the project root to compile
and test with Scala 2.13.18 and 3.10.1. Both builds use ZIO Streams 2.1.27.

See the [release notes](RELEASE.md) for the feature history.

