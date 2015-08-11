package pixelduck.akkastreamtest

import java.io.File

import scala.util.Random

object FillInput {

  def main (args: Array[String]){
    FillInput.run()
  }

  def run() {
    val userDir: String = System.getProperty("user.dir")
    val writableFile: File = IOUtils.asWritableFile(s"$userDir/src/main/resources/input.txt")
    IOUtils.printToFile(writableFile, append = false) { writer =>
      for (i <- 0 to 1000000) {
        val long: Long = Random.nextLong()
        writer.append(s"$long\n")
      }
    }
  }
}
