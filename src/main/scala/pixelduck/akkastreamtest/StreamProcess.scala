package pixelduck.akkastreamtest

import java.io.File

import akka.actor.{Actor, ActorRef, Props}
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import com.expedia.gps.geo.garfield.ShutdownActor.WatchMe
import pixelduck.akkastreamtest.StreamProcess.ProcessImport

import scala.concurrent.{ExecutionContext, Future}
import scala.io.{Source => ioSource}
import scala.util.Try


object StreamProcess {
  def props(shutdownActor: ActorRef, outputFile: String, nbThreads: Int): Props = Props(new StreamProcess(shutdownActor, outputFile, nbThreads))
  case class ProcessImport()
}

class StreamProcess(shutdownActor: ActorRef, outputFile: String, nbThreads: Int) extends Actor {

  shutdownActor ! WatchMe(context.self)

  val counters = new Counters()
  val writerActorRef: ActorRef = context.actorOf(ResultFileWriterActor.props(shutdownActor, outputFile), name = "WriterActor")

  def receive = {
    case msg: ProcessImport =>

      val settings = ActorMaterializerSettings(context.system).withInputBuffer(1, 1)
      implicit val materializer = ActorMaterializer(Some(settings))

      writerActorRef ! InitializeResults()
      import scala.concurrent.ExecutionContext.Implicits.global

      context.system.log.warning(s"Nb thread used: $nbThreads")

      val userDir: String = System.getProperty("user.dir")
      val inputFile: File = new File(s"$userDir/src/main/resources/input.txt")
      val run: Future[Int] = source(inputFile)
        .via(parse)
        .via(if (nbThreads == 1) enrich() else enrichAsync(nbThreads))
        .via(writeResults)
        .runWith(printProgress(writerActorRef))
      run.onComplete { result: Try[Int] =>
        context.system.log.info(s"Nb elements processed: ${result.get}")
        writerActorRef ! FinalizeResults()
      }
  }

  override def postStop() {
    printResults()
  }

  def source(cityJsonFile: File): Source[String, Unit] = {
    val source = ioSource.fromFile(cityJsonFile, "utf-8")
    Source(() => source.getLines())
  }

  def parse(implicit ec: ExecutionContext): Flow[String, Long, Unit]
    = Flow[String].map(doParse).collect {
        case Some(v) => v
      }.filter(_ % 2 == 0)

  def doParse(data: String): Option[Long] = Try {
    val ret = data.toLong
    counters.parsed.incrementAndGet()
    ret
  }.toOption

  def enrichAsync(nbThreads: Int)(implicit ec: ExecutionContext): Flow[Long, String, Unit]
      = Flow[Long].mapAsyncUnordered(nbThreads)(data => Future(doEnrich(data))).collect {
        case Some(data) => data
      }

  def enrich()(implicit ec: ExecutionContext): Flow[Long, String, Unit]
  = Flow[Long].map(doEnrich).collect {
    case Some(data) => data
  }

  def doEnrich(data: Long): Option[String] = {
    counters.enriched.incrementAndGet()
    Some(s"Test-$data")
  }

  def writeResults(implicit ec: ExecutionContext) : Flow[String, String, Unit]
      = Flow[String].map(data => {
        writerActorRef ! new AppendResult(data)
        counters.written.incrementAndGet()
        data
      }
      )

  def printProgress(writerActorRef: ActorRef): Sink[String, Future[Int]] = Sink.fold(0) { (count, data: String) =>
    if (count % 10000 == 0 && count != 0)
      context.system.log.info(s"$count elements written: $data")
    count + 1
  }

  private def printResults() {
    context.system.log.warning(
      s"""
When parsing ${counters.parsed.get()} entries in file,
- ${counters.enriched.get()} successfully enriched
- ${counters.written.get()} successfully written
""")
  }
}