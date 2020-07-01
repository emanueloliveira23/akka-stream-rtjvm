package part5advanced

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.Random

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.stage._

object CustomOperators extends App {

  implicit val system = ActorSystem("CustomOperators")
  implicit val materializer = ActorMaterializer()
  import system.dispatcher

  // 1 - a custom source which emits random numbers until cancelled

  class RandomNumberGenerator(max: Int) extends GraphStage[/*step 0: define the shape*/SourceShape[Int]] {

    // step 1: define the ports and the component-specific numbers
    val outPort = Outlet[Int]("randomGenerator")
    val random = new Random()

    // step 2: construct a new shape
    override def shape: SourceShape[Int] = SourceShape(outPort)

    // step 3: create the logic
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      // step 4: define mutable state
      // implement my logic here
      setHandler(outPort, new OutHandler {
        // when there is demands from downstream
        override def onPull(): Unit = {
          // emit a new element
          val nextNumber = random.nextInt(max)
          // push it out of the outPort
          push(outPort, nextNumber)
        }
      })
    }

  }

  val randomNumberGeneratorSource = Source.fromGraph(new RandomNumberGenerator(100))
//  randomNumberGeneratorSource.runWith(Sink.foreach(println))

  // 2 - a custom sink that prints elements in batches of a given size

  class Batcher(batchSize: Int) extends GraphStage[SinkShape[Int]] {

    val inPort = Inlet[Int]("batcher")

    override def shape: SinkShape[Int] = SinkShape[Int](inPort)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      // mutable state
      val batch = new collection.mutable.Queue[Int]

      override def preStart(): Unit = {
        pull(inPort)
      }

      setHandler(inPort, new InHandler {
        override def onPush(): Unit = {
          val nextElement = grab(inPort)
          batch.enqueue(nextElement)
          // assume some complex computation
           Thread.sleep(100)
          if (batch.size >= batchSize) {
            println("new batch: " + batch.dequeueAll(_ => true).mkString("[", ",", "]"))
          }
          pull(inPort) // send demand upstream
        }

        override def onUpstreamFinish(): Unit = {
          if (batch.nonEmpty) {
            println("new batch: " + batch.dequeueAll(_ => true).mkString("[", ",", "]"))
            println("Stream finished.")
          }
        }
      })
    }

  }

  val batchSink = Sink.fromGraph(new Batcher(10))
  // randomNumberGeneratorSource.to(batchSink).run()

  /**
    * Exercise: a custom flow - a simple filter flow
    * - 2 ports: an input port and output port and set it handlers
    */

  class SimpleFilterFlow[T](predicate: T => Boolean) extends GraphStage[FlowShape[T, T]] {

    val inPort = Inlet[T]("simpleFilterFlowInlet")
    val outPort = Outlet[T]("simpleFilterFlowOutlet")

    override def shape: FlowShape[T, T] = FlowShape[T, T](inPort, outPort)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

      setHandler(outPort, new OutHandler {
        override def onPull(): Unit = {
          pull(inPort)
        }
      })

      setHandler(inPort, new InHandler {
        override def onPush(): Unit = try {
          val nextElement = grab(inPort)
          if (predicate(nextElement)) push(outPort, nextElement) // pass it on
          else pull(inPort) // ask for another element
        } catch {
          case t: Throwable => failStage(t)
        }
      })

    }

  }

  val myFilter = Flow.fromGraph(new SimpleFilterFlow[Int](_ % 2 == 0))
  // randomNumberGeneratorSource.via(myFilter).to(batchSink).run()
  // backpressure OOTB!!


  /**
    * Materialize values in graph stages
    */

  // 3 - a flow thta counts the number of elements that go through it

  class CounterFlow[T] extends GraphStageWithMaterializedValue[FlowShape[T, T], Future[Int]] {

    val inPort = Inlet[T]("counterIn")
    val outPort = Outlet[T]("counterOut")

    override val shape: FlowShape[T, T] = FlowShape[T, T](inPort, outPort)

    override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Int]) = {
      val promise = Promise[Int]
      val logic = new GraphStageLogic(shape) {
        // set mutable state
        var counter = 0

        setHandler(outPort, new OutHandler {
          override def onPull(): Unit = {
            pull(inPort)
          }

          override def onDownstreamFinish(): Unit = {
            promise.success(counter)
            super.onDownstreamFinish()
          }
        })

        setHandler(inPort, new InHandler {
          override def onPush(): Unit = {
            val nextElement = grab(inPort)
            counter += 1
            push(outPort, nextElement)
          }

          override def onUpstreamFinish(): Unit = {
            promise.success(counter)
            super.onUpstreamFinish()
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            promise.failure(ex)
            super.onUpstreamFailure(ex)
          }
        })
      }

      (logic, promise.future)
    }
  }

  val counterFlow = Flow.fromGraph(new CounterFlow[Int])
  Source(1 to 10)
//    .map(x  => if (x == 7) throw new RuntimeException("gotcha") else x)
    .viaMat(counterFlow)(Keep.right)
//    .to(Sink.foreach(println))
    .to(Sink.foreach(x  => if (x == 7) throw new RuntimeException("gotcha, sink!") else println(x)))
    .run()
    .onComplete(println)


}
