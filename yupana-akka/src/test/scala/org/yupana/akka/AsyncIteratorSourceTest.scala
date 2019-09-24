package org.yupana.akka

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Sink, Source }
import akka.stream.testkit.scaladsl._
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings }
import akka.testkit.TestKit
import org.scalatest.{ FlatSpecLike, Matchers }

import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

class AsyncIteratorSourceTest extends TestKit(ActorSystem("Test")) with FlatSpecLike with Matchers {

  import system.dispatcher
  implicit val mat: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(system))

  "AsyncIteratorSource" should "finish when underlying iterator ends" in {
    val source = Source.fromGraph(new AsyncIteratorSource[Int](Iterator.range(1, 5), 10))

    source
      .runWith(TestSink.probe[Int])
      .request(10)
      .expectNext(1, 2, 3, 4)
      .expectComplete()
  }

  it should "support merge with infinite stream" in {
    val source = Source
      .fromIterator(() => Iterator.range(1, 5))
      .mapAsync(2) { i =>
        Future(i + 1)
      }
      .flatMapConcat(i => new AsyncIteratorSource[Int](Iterator.range(1, i), 3))

    val ticks = Source.tick(0.millis, 1.millis, 1).scan(0)(_ + _).drop(1)
    val merged = source.merge(ticks, eagerComplete = true)

    Await.ready(merged.runWith(Sink.foreach(print)), 1.second).isCompleted shouldEqual true
  }
}
