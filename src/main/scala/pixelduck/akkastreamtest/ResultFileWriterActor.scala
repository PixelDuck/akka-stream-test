package pixelduck.akkastreamtest

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import com.expedia.gps.geo.garfield.ShutdownActor.WatchMe
import com.typesafe.scalalogging.StrictLogging

/**
 * Write result on file system,
 */
object ResultFileWriterActor {
  def props(shutdownActor: ActorRef, outputFilePath: String): Props = Props(new ResultFileWriterActor(shutdownActor, outputFilePath))
}

private class ResultFileWriterActor(shutdownActor: ActorRef, outputFilePath: String) extends Actor with StrictLogging {

  shutdownActor ! WatchMe(context.self)

  lazy val outputFile = IOUtils.asWritableFile(outputFilePath, overwrite = true)

  def receive = {
    case msg: InitializeResults =>
      IOUtils.printToFile(outputFile, append=true) { writer =>
        writer.append("Results\n")
      }
    case msg: FinalizeResults =>
      //nothing to do except shutdown caller
      sender() ! PoisonPill
    case msg: AppendResult =>
      IOUtils.printToFile(outputFile, append=true) { writer =>
        writer.append(s"${msg.enrichedData}\n")
      }
    case _ => logger.warn("Message not handle by actor: $_")
  }

}

case class InitializeResults()
case class AppendResult(enrichedData: String)
case class FinalizeResults()
