package part2primer

import scala.util.{Failure, Success}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}

object MaterializingStreams extends App {

  implicit val system = ActorSystem("MaterializingStreams")
  implicit val materializer = ActorMaterializer()

  import system.dispatcher

  val simpleGraph = Source(1 to 10).to(Sink.foreach(println))
//  val simpleMaterializedValue = simpleGraph.run()

  val source = Source(1 to 10)
  val sink = Sink.reduce[Int](_ + _)
//  val sumFuture = source.runWith(sink)
//  sumFuture.onComplete {
//    case Success(value) => println(s"The sum of all elements is: $value")
//    case Failure(error) => println(s"The sum of elements could not be computed: $error")
//  }

  // choosing materialized values
  val simpleSource = Source(1 to 10)
  val simpleFlow = Flow[Int].map(_ + 1)
  val simpleSink = Sink.foreach[Int](println)
  val graph = simpleSource.viaMat(simpleFlow)(Keep.right).toMat(simpleSink)(Keep.right)
//  graph.run().onComplete {
//    case Success(_) => println("Stream processing finished")
//    case Failure(ex) => println(s"Stream processing failed: $ex")
//  }

  // sugars
  val sum = Source(1 to 10).runWith(Sink.reduce[Int](_ + _))
  // Future[Int]
//  Source(1 to 10).runReduce[Int](_ + _) // same

  // backwards
//  Sink.foreach[Int](println).runWith(Source.single(42))
  // both ways
//  Flow[Int].map(_ * 2).runWith(simpleSource, simpleSink)

  /**
    * Exercise
    * 1 - return the last element out of a source (use Sink.last)
    * 2 - compute the total word count out of a stream of sentences
    *   - map, fold, reduce
    */

  // 1
  val elements = Source(1 to 10)
  val last1 = elements.runWith(Sink.last)
  val last2 = elements.toMat(Sink.last)(Keep.right).run()
  val test = last2
  test.onComplete {
    case Success(last) => println(s"The last element is: $last")
    case Failure(ex) => println(s"An exception occurs while take last element: $ex")
  }

  // 2
  val sentencesSource = Source(List("I love Akka", "Akka Stream is awesome", "I'm proud of me"))
  val f1 = sentencesSource.map(_.split(" ")).fold(0)(_ + _.length).runWith(Sink.head)
  val wordCountSink = Sink.fold[Int, String](0)(_ + _.split(" ").length)
  sentencesSource.toMat(wordCountSink)(Keep.right).run()
  sentencesSource.runFold(0)(_ + _.split(" ").length)

  val wordCountFlow = Flow[String].fold[Int](0)(_ + _.split(" ").length)
  sentencesSource.via(wordCountFlow).toMat(Sink.head)(Keep.right).run()
  sentencesSource.viaMat(wordCountFlow)(Keep.none).toMat(Sink.head)(Keep.right).run()
  sentencesSource.via(wordCountFlow).runWith(Sink.head)
  wordCountFlow.runWith(sentencesSource, Sink.head)._2
}
