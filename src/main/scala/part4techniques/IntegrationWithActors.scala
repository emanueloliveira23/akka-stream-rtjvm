package part4techniques

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}

object IntegrationWithActors extends App {

  implicit val system = ActorSystem("IntegrationWithActors")
  implicit val materializer = ActorMaterializer()

  class SimpleActor extends Actor with ActorLogging {
    def receive: Receive = {
      case s: String =>
        log.info(s"Just received a string: $s")
        sender() ! s"$s$s"
      case n: Int =>
        log.info(s"Just received a number: $n")
        sender() ! (2 * n)
    }
  }

  val simpleActor = system.actorOf(Props[SimpleActor], "simpleActor")

  val numberSource = Source(1 to 10)

  // actor as a flow
  import akka.util.Timeout
  import scala.concurrent.duration._
  implicit val timeout = Timeout(2 seconds)
  val actorFlow = Flow[Int].ask[Int](parallelism = 4)(simpleActor)

//  numberSource.via(actorFlow).to(Sink.ignore).run()
//  numberSource.ask[Int](parallelism = 4)(simpleActor).to(Sink.ignore).run()

  /*
    Actor as a source
   */
  val actorPoweredSource = Source.actorRef[Int](bufferSize = 10, overflowStrategy = OverflowStrategy.dropHead)
  val materializedActorRef = actorPoweredSource.to(Sink.foreach[Int](n => println(s"Actor powered flow got number: $n"))).run()
  materializedActorRef ! 10
  // terminate the stream
  materializedActorRef ! akka.actor.Status.Success("complete")

  /*
    Actor as a destination/sink
    - an init message
    - an ack message to confirm the reception
    - a complete message
    - a function to generate a message in case the stream throws an exception
   */

  case object StreamInit
  case object StreamAck
  case object StreamComplete
  case class StreamFail(ex: Throwable)

  class DestinationActor extends Actor with ActorLogging {
    def receive: Receive = {
      case StreamInit =>
        log.info("Stream initialized")
        sender() ! StreamAck
      case StreamComplete =>
        log.info("Stream complete")
        context.stop(self)
      case StreamFail(ex) =>
        log.warning(s"Stream failed: $ex")
      case message =>
        log.info(s"Message $message has come to its final resting point")
        sender() ! StreamAck
    }
  }

  val destinationActor = system.actorOf(Props[DestinationActor], "destinationActor")

  val actorPoweredSink = Sink.actorRefWithAck[Int](
    destinationActor,
    onInitMessage = StreamInit,
    ackMessage = StreamAck,
    onCompleteMessage = StreamComplete,
    onFailureMessage = StreamFail // optional
  )

  Source(1 to 10).to(actorPoweredSink).run()

  // Sink.actorRef() not recommended, unable to backpressure

}
