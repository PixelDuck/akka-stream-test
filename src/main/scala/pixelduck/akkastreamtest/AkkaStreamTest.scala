package com.expedia.gps.geo.garfield

import akka.actor._
import com.expedia.gps.geo.garfield.ShutdownActor.WatchMe
import com.typesafe.config.ConfigFactory
import dispatch.Http
import pixelduck.akkastreamtest.StreamProcess
import pixelduck.akkastreamtest.StreamProcess.ProcessImport

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Main class.
 */
object AkkaStreamTest {

  val start = System.currentTimeMillis()
  val system = ActorSystem("akka-stream-system", ConfigFactory.load())

  case class CommandParameter(nbThreads: Int = 1, outputFile: String = "/tmp/result.txt")

  val parser = new scopt.OptionParser[CommandParameter]("akka-stream-test") {
    head("Akka stream test", "1.x")
    help("help") text "prints this usage text"
    opt[Int]('t', "nbThreads") valueName "<nbThreads>" action { (x, c) =>
      c.copy(nbThreads = x)
    } text "set the number of threads to use to enrich data"
    opt[String]('o', "out") valueName "<file>" action { (x, c) =>
      c.copy(outputFile = x) } text "out is a required file property"
  }

  def main(args: Array[String]) {
    val parameter = parser.parse(args, CommandParameter()).get

    implicit val shutdownActor = system.actorOf(ShutdownActor.props(), name = "ShutdownActor")
    val streamProcess = system.actorOf(StreamProcess.props(shutdownActor, parameter.outputFile, parameter.nbThreads), name = "StreamProcess")
    streamProcess ! ProcessImport()
  }
}

object ShutdownActor {
  def props(): Props = Props(new ShutdownActor())

  case class WatchMe(ref: ActorRef)
}

class ShutdownActor extends Actor {

  val watched = ArrayBuffer.empty[ActorRef]

  final def receive = {
    case WatchMe(ref) =>
      context.watch(ref)
      watched += ref
      context.system.log.info(s"Will check $ref. actual monitored actors: $watched")
    case Terminated(ref) =>
      watched -= ref
      context.system.log.info(s"$ref is no more monitored. pending monitors: $watched")
      if (watched.isEmpty) shutdown()
  }

  def shutdown() {
    val duration: FiniteDuration = Duration.create(System.currentTimeMillis() - AkkaStreamTest.start, MILLISECONDS)
    context.system.log.warning(s"Finished in ${duration.toCoarsest}")

    context.system.log.warning("System shutdown")
    Http.shutdown()
    context.system.shutdown()
    System.exit(0)
  }
}
