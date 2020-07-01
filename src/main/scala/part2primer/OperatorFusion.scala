package part2primer

import akka.actor.{Actor, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}

object OperatorFusion extends App {

  implicit val system = ActorSystem("OperatorFusion")
  implicit val materializer = ActorMaterializer()

  val simpleSource = Source(1 to 1000)
  val simpleFlow = Flow[Int].map(_ + 1)
  val simpleFlow2 = Flow[Int].map(_ * 10)
  val simpleSink = Sink.foreach(println)

  // this runs on the SAME ACTOR
  // simpleSource.via(simpleFlow).via(simpleFlow2).to(simpleSink).run()
  // operator/component FUSION

  // "equivalent" behavior
  class SimpleActor extends Actor {
    def receive: Receive = {
      case x: Int =>
        val x2 = x + 1
        val y = x2 * 10
        println(y)
    }
  }
  val simpleActor = system.actorOf(Props[SimpleActor])
  // (1 to 1000).foreach(simpleActor ! _)

  // complex operators
  val complexFlow = Flow[Int].map { x =>
    Thread.sleep(1000)
    x + 1
  }

  val complexFlow2 = Flow[Int].map { x =>
    Thread.sleep(1000)
    x * 10
  }

  // simpleSource.via(complexFlow).via(complexFlow2).to(simpleSink).run()

  // async boundary
//  simpleSource.via(complexFlow).async // runs on one actor
//    .via(complexFlow2).async // runs on another actor
//    .to(simpleSink) // runs on a third actor
//    .run()

  // ordering guarantees
  Source(1 to 3)
    .map { e =>
      println(s"Flow A: $e")
      e
    }
    .async
    .map { e =>
      println(s"Flow B: $e")
      e
    }
    .async
    .map { e =>
      println(s"Flow C: $e")
      e
    }
    .async
    .runWith(Sink.ignore)

}
