package streamextension

import zio.{Duration, Schedule}
import zio.stream.{ZSink, ZStream}

object syntax {
  implicit final class KeyedStreamOps[R, E, A](private val stream: ZStream[R, E, A]) extends AnyVal {
    def throttleLatestByKey[K](window: Duration)(key: A => K): ZStream[R, E, A] =
      stream.groupByKey(key) { (_, events) =>
        events
          .aggregateAsync(ZSink.foldLeft[A, Option[A]](None)((_, event) => Some(event)))
          .collectSome
          .throttleShape(1, window)(_.size.toLong)
      }

    def conflateByKey[K](window: Duration)(key: A => K): ZStream[R, E, A] =
      stream
        .aggregateAsyncWithin(
          ZSink.foldLeft[A, Map[K, A]](Map.empty)((latest, event) => latest.updated(key(event), event)),
          Schedule.spaced(window)
        )
        .mapConcat(_.values)
  }
}
