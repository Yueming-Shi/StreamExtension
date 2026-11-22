package streamextension

import java.time.{Instant, LocalTime, ZoneId, ZoneOffset}
import streamextension.timeGateSyntax._
import zio._
import zio.stream.ZStream
import zio.test._

object TimeGateSpec extends ZIOSpecDefault {
  private val noon = LocalTime.NOON
  private val utc = ZoneOffset.UTC
  private val toronto = ZoneId.of("America/Toronto")

  def spec = suite("time gate extensions")(
    test("cutoff and window boundaries have nanosecond precision") {
      val before = noon.minusNanos(1)
      val after = noon.plusNanos(1)
      assertTrue(
        !isAfterCutoff(noon)(noon), isAfterCutoff(noon)(after),
        untilCutoff(noon)(before) == java.time.Duration.ofNanos(1),
        untilCutoff(noon)(noon).isZero, untilCutoff(noon)(after).isZero,
        isInWindow(before, noon)(before), isInWindow(before, noon)(noon),
        !isInWindow(before, noon)(after), isInWindow(noon, noon)(noon),
        !isInWindow(noon, noon)(before), !isInWindow(noon, noon)(after)
      )
    },
    test("daily cutoff includes equality and reopens at local midnight") {
      val times = List("2025-01-01T17:00:00Z", "2025-01-01T17:00:01Z", "2025-01-02T05:00:00Z")
      ZStream.fromIterable(times).mapZIO(t => TestClock.setTime(Instant.parse(t)).as(t))
        .dailyCutoff(noon, toronto).runCollect
        .map(result => assertTrue(result.toList == List(times.head, times.last)))
    },
    test("overnight windows include both endpoints in both granularities") {
      val times = List("22:00", "00:00", "06:00", "12:00").map(LocalTime.parse(_))
      val stream = ZStream.fromIterable(times).mapZIO { t =>
        TestClock.setTime(Instant.EPOCH.plusNanos(t.toNanoOfDay)).as(t)
      }
      ZIO.foreach(List(Granularity.PerChunk, Granularity.PerElement)) { g =>
        stream.withinTimeWindow(times.head, times(2), utc, g).runCollect
      }.map(results => assertTrue(results.forall(_.toList == times.take(3))))
    },
    test("an elapsed deadline never starts upstream, including at equality") {
      for {
        started <- Ref.make(false)
        stream = ZStream.fromZIO(started.set(true))
        _ <- TestClock.setTime(Instant.parse("2025-01-01T12:00:00Z"))
        _ <- stream.haltAtCutoff(noon, utc).runDrain
        _ <- TestClock.adjust(1.second)
        _ <- stream.haltAtCutoff(noon, utc).runDrain
        value <- started.get
      } yield assertTrue(!value)
    },
    test("a cutoff crossed inside one chunk drops its tail only in per-element mode") {
      def run(mode: Granularity) = {
        val reads = new java.util.concurrent.atomic.AtomicLong()
        val ticks: java.time.InstantSource = () => Instant.EPOCH.plusSeconds(reads.getAndIncrement())
        ZStream.fromChunk(Chunk(1, 2, 3)).dailyCutoff(LocalTime.MIDNIGHT, utc, mode)
          .runCollect.withClock(Clock.ClockJava(ticks.withZone(utc)))
      }
      for {
        chunk <- run(Granularity.PerChunk)
        elements <- run(Granularity.PerElement)
      } yield assertTrue(chunk == Chunk(1, 2, 3), elements == Chunk(1))
    },
    test("a cutoff in a DST gap expires at the jump and a repeated cutoff expires on its first occurrence") {
      def skippedAt(time: String, cutoff: LocalTime) =
        TestClock.setTime(Instant.parse(time)) *>
          ZStream.succeed(1).haltAtCutoff(cutoff, toronto).runCollect
      for {
        gap <- skippedAt("2025-03-09T07:00:00Z", LocalTime.of(2, 30))
        overlap <- skippedAt("2025-11-02T06:00:00Z", LocalTime.of(1, 30))
      } yield assertTrue(gap.isEmpty, overlap.isEmpty)
    },
    test("idle upstream halts on the clock across the spring-forward transition") {
      for {
        _ <- TestClock.setTime(Instant.parse("2025-03-09T06:30:00Z"))
        started <- Promise.make[Nothing, Unit]
        fiber <- (ZStream.fromZIO(started.succeed(())) ++ ZStream.never)
          .haltAtCutoff(LocalTime.of(3, 30), toronto).runDrain.fork
        _ <- started.await
        _ <- TestClock.adjust(59.minutes)
        before <- fiber.poll
        _ <- TestClock.adjust(1.minute)
        after <- fiber.await.timeout(1.second).fork
        _ <- TestClock.adjust(1.second)
        result <- after.join
      } yield assertTrue(before.isEmpty, result.exists(_.isSuccess))
    }
  )
}
