package org.yupana.akka

import akka.actor.ActorSystem
import akka.stream.ClosedShape
import akka.stream.scaladsl._
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.TestKit
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class ValveTest extends TestKit(ActorSystem("Test")) with AnyFlatSpecLike with Matchers {

  import GraphDSL.Implicits._

  "Valve" should "provide limited number of items" in {
    val ctrl = TestSource.probe[Int]

    val probe = TestSubscriber.manualProbe[Int]()

    val ctrlIn = RunnableGraph
      .fromGraph(GraphDSL.createGraph(ctrl) { implicit b => c =>
        val valve = b.add(new Valve[Int])

        val s = b.add(Source(1 to 100))

        s ~> valve.in0
        c ~> valve.in1
        valve.out ~> Sink.fromSubscriber(probe)

        ClosedShape
      })
      .run()

    val subscription = probe.expectSubscription()
    ctrlIn.sendNext(3)

    subscription.request(5)
    probe.expectNext(1, 2, 3)

    probe.expectNoMessage()
    ctrlIn.sendNext(4)
    probe.expectNext(4, 5)
    probe.expectNoMessage()
  }

}
