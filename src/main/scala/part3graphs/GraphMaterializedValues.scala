package part3graphs

import scala.concurrent.Future
import scala.util.{Failure, Success}

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, FlowShape, SinkShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Sink, Source}

object GraphMaterializedValues extends App {

  implicit val system = ActorSystem("GraphMaterializedValues")
  implicit val materializer = ActorMaterializer()

  val wordSource = Source(List("AKka", "is", "awesome", "rock", "the", "jvm"))
  val printer = Sink.foreach[String](println)
  val counter = Sink.fold[Int, String](0)((c, _) => c + 1)

  /*
    A composite component (sink)
    - prints out all string which are lowercase
    - COUNTS the strings that are short (< 5 chars)
   */

  // step 1
  val complexWordSink = Sink.fromGraph(
    GraphDSL.create(printer, counter)(Keep.right) { implicit builder => (printerShape, counterShape) =>
      import GraphDSL.Implicits._

      // step 2 - components
      val broadcast = builder.add(Broadcast[String](2))
      val lowercaseFilter = builder.add(Flow[String].filter(_.forall(_.isLower)))
      val shortFilter = builder.add(Flow[String].filter(_.length < 5))

      // step 3 - connections
      broadcast ~> lowercaseFilter ~> printerShape
      broadcast ~> shortFilter ~> counterShape

      // step 4 - the shape
      SinkShape(broadcast.in)
    }
  )

  import system.dispatcher
//  val shortStringsCountFuture = wordSource.toMat(complexWordSink)(Keep.right).run()
//  shortStringsCountFuture.onComplete {
//    case Success(count) => println(s"The total number of short strings is: $count")
//    case Failure(exception) => println(s"The total number of short strings failed: $exception")
//  }

  /**
    * Exercise
    */
  def enhanceFlow[A, B](flow: Flow[A, B, _]): Flow[A, B, Future[Int]] = {

    val counter = Sink.fold[Int, B](0)((c, _) => c + 1)

    // step 1
    Flow.fromGraph(
      GraphDSL.create(counter) { implicit builder => counterShape =>
        import GraphDSL.Implicits._

        // step 2 - components
        val broadcast = builder.add(Broadcast[B](2))
        val originalFlowShape = builder.add(flow)

        // step 3 - connections
        originalFlowShape ~> broadcast ~> counterShape

        // step 4 - shape
        FlowShape(originalFlowShape.in, broadcast.out(1))
      }
    )

  }

  /*
    Hint: use a broadcast and a Sink.fold
   */

  val simpleSource = Source(1 to 42)
  val simpleFlow = Flow[Int].map(identity)
  val simpleSink = Sink.ignore

  simpleSource.viaMat(enhanceFlow(simpleFlow))(Keep.right).toMat(simpleSink)(Keep.left).run().onComplete {
    case Success(count) => println(s"Number of elements: $count")
    case Failure(exception) => println(s"Fail to count elements: $exception")
  }

}
