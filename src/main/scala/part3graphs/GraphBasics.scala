package part3graphs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.stream.scaladsl.{Balance, Broadcast, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source, Zip}

object GraphBasics extends App {

  implicit val system = ActorSystem("GraphBasics")
  implicit val materializer = ActorMaterializer()

  val input = Source(1 to 1000)
  val incrementer = Flow[Int].map(_ + 1) // hard computation
  val multiplier = Flow[Int].map(_ * 10) // hard computation
  val output = Sink.foreach[(Int, Int)](println)

  // step 1 - setting up the fundamentals for the graph
  val graph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] => // builder = MUTABLE data structure
      import GraphDSL.Implicits._ // some nice operator into scope

      // step 2 - add the necessary components to this graph
      val broadcast = builder.add(Broadcast[Int](2)) // fan-out operator
      val zip = builder.add(Zip[Int, Int]) // fan-in operator

      // step 3 - trying up the components
      input ~> broadcast
      broadcast.out(0) ~> incrementer ~> zip.in0
      broadcast.out(1) ~> multiplier ~> zip.in1
      zip.out ~> output

      // step 4 - return a closed shape
      ClosedShape // FREEZE the builder's shape
      // shape
    } // static graph
  ) // runnable graph

  // graph.run() // run the graph and materialize it

  /**
    * Exercise 1 - Feed a source into 2 sinks at the same time (use a broadcast)
    */
  val sink1 = Sink.foreach[Int](i => println(s"Sink 1: $i"))
  val sink2 = Sink.foreach[Int](j => println(s"Sink 2: $j"))
  val graph2 = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val broadcast = builder.add(Broadcast[Int](2))

      input ~> broadcast
      // broadcast.out(0) ~> sink1
      // broadcast.out(1) ~> sink2
      // or via implicit por
      broadcast ~> sink1
      broadcast ~> sink2
      ClosedShape
    }
  )
  // graph2.run()

  /**
    * Exercise 2 - build from image
    */
  import scala.concurrent.duration._
  val fastSource = input.throttle(5, 1 second)
  val slowSource = input.throttle(2, 1 second)
  val sinkCount1 = Sink.fold[Int, Int](0) { (count, _) =>
    println(s"Sink 1 number of elements: $count")
    count + 1
  }
  val sinkCount2 = Sink.fold[Int, Int](0) { (count, _) =>
    println(s"Sink 2 number of elements: $count")
    count + 1
  }
  val complexGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._
      val merge = builder.add(Merge[Int](2))
      val balance = builder.add(Balance[Int](2))
      fastSource ~> merge ~> balance ~> sinkCount1
      slowSource ~> merge; balance ~> sinkCount2
      ClosedShape
    }
  )
  complexGraph.run()

}
