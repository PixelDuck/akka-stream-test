package pixelduck.akkastreamtest

import java.io._

import scala.io.Source

object IOUtils {

  def parseResource(resourcePath: String, skipFirst: Boolean = false)(parseLine: (String) => Unit){
    var first = true
    for (line <- Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(resourcePath)).getLines()) {
      if (skipFirst && first)
        first = false
      else {
        val lineTrimed = line.trim
        if (lineTrimed.nonEmpty)
          parseLine(lineTrimed)
      }
    }
  }

  def printToFile(f: java.io.File, append: Boolean = false, charset: String = "utf-8")(op: Writer => Unit) {
    val fos = new FileOutputStream(f, append)
    val osw = new OutputStreamWriter(fos, charset)
    val writer: BufferedWriter = new BufferedWriter(osw)
    try { op(writer) } finally {
      writer.close()
    }
  }

  def asWritableFile(path: String, overwrite: Boolean = false): File = {
    val f = new File(path)
    if (! f.exists()) {
      if (f.getParentFile.exists())
        f.getParentFile.mkdirs()
      f.createNewFile()
    }
    if (overwrite)
      f.delete()
      f.createNewFile()
    f
  }
}
