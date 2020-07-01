package part2primer

import scala.concurrent.Future

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}

object FirstPrinciples extends App {

  implicit val system = ActorSystem("FirstPrinciples")
  implicit val materializer = ActorMaterializer()

  // sources
  val source = Source(1 to 10)
  // sinks
  val sink = Sink.foreach[Int](println)

  val graph = source.to(sink)
//  graph.run()

  // flows transform elements
  val flow = Flow[Int].map(_ * 2)
  val sourceWithFlow = source.via(flow)
  val flowWithSink = flow.to(sink)

//  sourceWithFlow.to(sink).run()
//  source.to(flowWithSink).run()
//  source.via(flow).to(sink).run()

  // null are NOT allowed
//  val illegalSource = Source.single[String](null)
//  illegalSource.to(Sink.foreach(println)).run()
  // use Option instead

  // various kind of sources
  val finiteSource = Source.single(1)
  val anotherFiniteSource = Source(List(1, 2, 3))
  val emptySource = Source.empty[Int]
  val infiniteSource = Source(Stream.from(1)) // do not confuse an Akka stream with a "collection" Stream
  import scala.concurrent.ExecutionContext.Implicits.global
  val futureSource = Source.fromFuture(Future(42))

  // sinks
  val theMostBoringSink = Sink.ignore
  val foreachSink = Sink.foreach[String](println)
  val headSink = Sink.head[Int] // retrieves the head and then closes the stream
  val foldSink = Sink.fold[Int, Int](0)(_ + _)

  // flows - usually mapped to collection operators
  val mapFlow = Flow[Int].map(_ * 2)
  val takeFlow = Flow[Int].take(5)
  // drop, filter
  // NOT have flatMap

  // source -> flow -> flow -> ... -> sink
  val doubleFlowGraph = source.via(mapFlow).via(takeFlow).to(sink)
//  doubleFlowGraph.run()

  // syntactic sugar
  // My opinion: Isn't syntactic sugar. It's just functions. For-comprehension is syntactic sugar. :p
  val mapSource = Source(1 to 10).map(_ * 2) // Source(1 to 10).via(Flow[Int].map(_ * 2))
  // run streams directly
//  mapSource.runForeach(println) // Source(1 to 10).to(Source.foreach(println)).run()

  // OPERATORS = components

  /**
    * Exercise: create a stream that takes the name of persons, then you will keep the first 2 names with length > 5 chars
    */
  val names = List("João", "Emanuel", "Leila", "Eduardo", "Ilanna", "Oliveira")
  val namesSource = Source(names)
  val longNameFlow = Flow[String].filter(_.length > 5)
  val namesLimit = Flow[String].take(2)
  val namesSink = Sink.foreach(println)
  // run
  namesSource.via(longNameFlow).via(namesLimit).to(namesSink).run()
  // simpler
  // namesSource.filter(_.length > 5).take(2).runForeach(println)
}
