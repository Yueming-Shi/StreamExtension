package streamextension

import zio._
import zio.stream.ZStream
import zio.test._
import streamextension.keyedStreamSyntax._

object KeyedStreamSpec extends ZIOSpecDefault {
  def spec = suite("keyed stream extensions")(
    test("conflation flushes the latest value for each key on completion") {
      ZStream(("a", 1), ("b", 2), ("a", 3))
        .conflateByKey(1.hour)(_._1).runCollect
        .map(values => assertTrue(values.length == 2, values.toSet == Set(("a", 3), ("b", 2))))
    },
    test("each key can emit immediately on its own throttle") {
      ZStream("a", "b").throttleLatestByKey(1.hour)(identity).runCollect
        .map(values => assertTrue(values.length == 2, values.toSet == Set("a", "b")))
    },
    test("empty input produces no output") {
      val empty: ZStream[Any, Nothing, Int] = ZStream.empty
      for {
        conflated <- empty.conflateByKey(1.second)(identity).runCollect
        throttled <- empty.throttleLatestByKey(1.second)(identity).runCollect
      } yield assertTrue(conflated.isEmpty, throttled.isEmpty)
    }
  )
}
