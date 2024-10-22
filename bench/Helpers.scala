package tyql.bench
import scala.io.Source
import java.io.File
import java.nio.file.{Path, Paths, Files}
import scala.jdk.CollectionConverters._

object Helpers {
  def readDDLFile(filePath: String): Seq[String] = {
    val src = Source.fromFile(new File(filePath))
    val fileContents = src.getLines().mkString("\n")
    val result = fileContents.split(";").map(_.trim).filter(_.nonEmpty).toSeq
    src.close()
    result
  }

  def getCSVFiles(directoryPath: String): Seq[Path] = {
    val dirPath = Paths.get(directoryPath)
    if (Files.isDirectory(dirPath)) {
      Files.list(dirPath)
        .iterator()
        .asScala
        .filter(path => Files.isRegularFile(path) && path.toString.endsWith(".csv"))
        .toSeq
    } else {
      throw new Exception(s"$directoryPath is not a directory")
    }
  }
}