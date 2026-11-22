package streamextension

import java.time.{Duration => JDuration, LocalTime, ZoneId}
import zio.{Chunk, Clock, Ref, UIO, ZIO}
import zio.stream.ZStream

object timeGateSyntax {
  def isAfterCutoff(cutoff: LocalTime)(now: LocalTime): Boolean = now.isAfter(cutoff)

  /** Inclusive at both ends. A reversed interval spans midnight and equal ends select one instant. */
  def isInWindow(start: LocalTime, end: LocalTime)(now: LocalTime): Boolean =
    if (end.isBefore(start)) !now.isBefore(start) || !now.isAfter(end)
    else !now.isBefore(start) && !now.isAfter(end)

  /** Same-day difference in wall-clock time, clamped to zero once the cutoff passes. */
  def untilCutoff(cutoff: LocalTime)(now: LocalTime): JDuration =
    if (now.isAfter(cutoff)) JDuration.ZERO else JDuration.between(now, cutoff)

  def localTimeIn(zone: ZoneId): UIO[LocalTime] =
    Clock.instant.map(_.atZone(zone).toLocalTime)

  sealed trait Granularity
  object Granularity {
    case object PerChunk extends Granularity
    case object PerElement extends Granularity
  }

  implicit final class TimeGateOps[R, E, T](private val stream: ZStream[R, E, T]) extends AnyVal {
    /** Pass data through the cutoff, inclusive. The gate reopens at local midnight. */
    def dailyCutoff(
      cutoff: LocalTime,
      zone: ZoneId,
      granularity: Granularity = Granularity.PerChunk
    ): ZStream[R, E, T] =
      gateByLocalTime(stream, zone, granularity)(now => !isAfterCutoff(cutoff)(now)) { (open, now) =>
        s"Daily cutoff gate is ${if (open) "OPEN" else "CLOSED"} at $now $zone (cutoff $cutoff)"
      }

    /** Halt at today's cutoff even while idle. In a clock gap, halt when the clock jumps forward.
      * In an overlap, use the first occurrence. An elapsed deadline skips upstream entirely.
      */
    def haltAtCutoff(cutoff: LocalTime, zone: ZoneId): ZStream[R, E, T] =
      ZStream.unwrap(Clock.instant.map { now =>
        val localDeadline = now.atZone(zone).toLocalDate.atTime(cutoff)
        val transition = zone.getRules.getTransition(localDeadline)
        val deadline =
          if (transition != null && transition.isGap) transition.getInstant
          else localDeadline.atZone(zone).toInstant
        val reached = ZIO.logInfo(s"Cutoff $cutoff reached in $zone, stream halted")
        if (!now.isBefore(deadline)) ZStream.fromZIO(reached).drain
        else stream.interruptWhen(
          Clock.instant.flatMap(t => ZIO.sleep(JDuration.between(t, deadline)).when(t.isBefore(deadline))) *> reached
        )
      })

    /** Pass data during the inclusive local-time window each day. */
    def withinTimeWindow(
      start: LocalTime,
      end: LocalTime,
      zone: ZoneId,
      granularity: Granularity = Granularity.PerChunk
    ): ZStream[R, E, T] =
      gateByLocalTime(stream, zone, granularity)(isInWindow(start, end)) { (open, now) =>
        s"Time window is ${if (open) "OPEN" else "CLOSED"} at $now $zone (window $start - $end)"
      }
  }

  private def gateByLocalTime[R, E, T](stream: ZStream[R, E, T], zone: ZoneId, granularity: Granularity)(
    isOpen: LocalTime => Boolean
  )(describe: (Boolean, LocalTime) => String): ZStream[R, E, T] =
    ZStream.unwrap(Ref.make(Option.empty[Boolean]).map { lastOpen =>
      val check = for {
        now <- localTimeIn(zone)
        open = isOpen(now)
        previous <- lastOpen.getAndSet(Some(open))
        _ <- ZIO.logInfo(describe(open, now)).unless(previous.contains(open))
      } yield open

      granularity match {
        case Granularity.PerChunk =>
          stream.mapChunksZIO { chunk =>
            if (chunk.isEmpty) ZIO.succeed(chunk)
            else check.map(open => if (open) chunk else Chunk.empty)
          }
        case Granularity.PerElement => stream.filterZIO(_ => check)
      }
    })
}
