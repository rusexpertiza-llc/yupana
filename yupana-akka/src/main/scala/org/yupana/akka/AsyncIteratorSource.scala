package org.yupana.akka

import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class AsyncIteratorSource[T](val iterator: Iterator[T], bufferCapacity: Int)(implicit val executionContext: ExecutionContext) extends GraphStage[SourceShape[T]] {
  val out: Outlet[T] = Outlet("AsyncIteratorSource")
  override val shape: SourceShape[T] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =

    new GraphStageLogic(shape) {
      private val buffer = new collection.mutable.Queue[T]()

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {

          if (buffer.nonEmpty) {
            push(out, buffer.dequeue())
          } else {
            val f = Future {
              iterator.take(bufferCapacity).foreach(buffer.enqueue(_))
            }

            val callback = getAsyncCallback[Try[Unit]] {
              case Success(_) if buffer.nonEmpty =>
                val v = buffer.dequeue()
                push(out, v)
              case Success(_) =>
                completeStage()
              case Failure(e) =>
                failStage(e)
            }

            f.onComplete(callback.invoke)
          }
        }
      })
    }
}
