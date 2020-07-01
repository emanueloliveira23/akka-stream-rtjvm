package part5advanced

import scala.util.Success

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}

object Substreams extends App {

  implicit val system = ActorSystem("Substreams")
  implicit val materializer = ActorMaterializer()
  import system.dispatcher

  // 1 - grouping a stream by a certain function
  val wordsSource = Source(List("Akka", "is", "amazing", "learning", "substreams"))
  val groups = wordsSource.groupBy(30, w => if (w.isEmpty) '\0' else w.toLowerCase.head)

  groups.to(Sink.fold(0) { (count, word) =>
    val newCount = count + 1
    println(s"I just received $word, count is $newCount")
    newCount
  })//.run()

  // 2 - merge substreams back
  val textSource = Source(List(
    "I love Akka Streams",
    "this is amazing",
    "learning from Rock the JVM"
  ))

  val totalCharsCountFuture = textSource.groupBy(20, _.length % 2)
    .map(_.length) // do your expensive computation here
    .mergeSubstreamsWithParallelism(2)
    .toMat(Sink.reduce[Int](_ + _))(Keep.right)
//    .run()
//  totalCharsCountFuture.onComplete {
//    case Success(value) => println(s"Total char count: $value")
//    case _ => // ... wont't fail
//  }

  // 3 - splitting a stream into substreams, when a condition is met

  val text = "I love Akka Streams\n" +
    "this is amazing\n"+
    "learning from Rock the JVM\n"
  val anotherCharCountFuture = Source(text.toList)
    .splitWhen(_ == '\n')
    .filterNot(_ == '\n')
    .map(_ => 1)
    .mergeSubstreams
    .toMat(Sink.reduce[Int](_ + _))(Keep.right)
//    .run()
//  anotherCharCountFuture.onComplete {
//    case Success(value) => println(s"Total char count: $value")
//    case _ => // ... wont't fail
//  }

  // 4 - flatting

  val simpleSource = Source(1 to 5)
  simpleSource.flatMapConcat(x => Source(x to (3 * x))).runWith(Sink.foreach(println))
  simpleSource.flatMapMerge(2, x => Source(x to (3 * x))).runWith(Sink.foreach(println))


}
