package part4techniques

import java.util.Date

import scala.concurrent.Future

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout

object IntegratingWithExternalServices extends App {

  implicit val system = ActorSystem("IntegratingWithExternalServices")
  implicit val materializer = ActorMaterializer()
  // import system.dispatcher // this is not recommend in practice for mapAsync
  implicit val dispatcher = system.dispatchers.lookup("dedicated-dispatcher")

  def genericExternalService[A, B](element: A): Future[B] = ???

  // example: simplified PagerDuty
  case class PageEvent(app: String, desc: String, date: Date)

  val eventSource = Source(List(
    PageEvent("AkkaInfra", "Infra broke", new Date),
    PageEvent("FastDataPipeline", "Illegal elements in the data pipeline", new Date),
    PageEvent("AkkaInfra", "A service stopped responding", new Date),
    PageEvent("SuperFrontend", "A button doesn't work", new Date),
  ))

  object PagerService {
    private val engineers = List("Daniel", "John", "Lady Gaga")
    private val emails = Map(
      "Daniel" -> "dan@gmail.com",
      "John" -> "degoes@gmail.com",
      "Lady Gaga" -> "badromance@hotmail.com",
    )
    def processEvent(pageEvent: PageEvent) = Future {
      val engineerIndex = (pageEvent.date.toInstant.getEpochSecond / (24 * 3600)) % engineers.length
      val engineer = engineers(engineerIndex.intValue())
      val engineerEmail = emails(engineer)

      // page the engineer
      println(s"Sending engineer $engineerEmail a high priority notification: $pageEvent")
      Thread.sleep(1000)

      engineerEmail
    }
  }

  val infraEvents = eventSource.filter(_.app == "AkkaInfra")
  val pagedEngineerEmails = infraEvents.mapAsync(parallelism = 1)(PagerService.processEvent)
  // guarantees the relative order of elements
  val pagedEmailSink = Sink.foreach[String](email => println(s"Successfully sent notification to $email"))
//  pagedEngineerEmails.to(pagedEmailSink).run()


  class PagerActor extends Actor with ActorLogging {
    private val engineers = List("Daniel", "John", "Lady Gaga")
    private val emails = Map(
      "Daniel" -> "dan@gmail.com",
      "John" -> "degoes@gmail.com",
      "Lady Gaga" -> "badromance@hotmail.com",
    )

    def receive: Receive = {
      case event: PageEvent =>
        sender() ! processEvent(event)
    }

    private def processEvent(pageEvent: PageEvent) = {
      val engineerIndex = (pageEvent.date.toInstant.getEpochSecond / (24 * 3600)) % engineers.length
      val engineer = engineers(engineerIndex.intValue())
      val engineerEmail = emails(engineer)

      // page the engineer
      log.info(s"Sending engineer $engineerEmail a high priority notification: $pageEvent")
      Thread.sleep(1000)

      engineerEmail
    }
  }

  import akka.pattern.ask
  import scala.concurrent.duration._
  implicit val timeout = Timeout(3 seconds)
  val pagerActor = system.actorOf(Props[PagerActor], "pagerActor")
  val altPagedEngineerEmails = infraEvents.mapAsync(parallelism = 4)(e => (pagerActor ? e).mapTo[String])
  altPagedEngineerEmails.to(pagedEmailSink).run()

  // do not confuse mapAsync with async (ASYNC boundary)

}
