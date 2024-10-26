package tyql.bench
import scala.io.Source
import java.io.File
import java.nio.file.{Path, Paths, Files}
import scala.jdk.CollectionConverters._
import java.io.{BufferedWriter, FileWriter}
import java.sql.{ResultSet, ResultSetMetaData}

object Helpers {
  val currentData = "data_100mb"
  def readDDLFile(filePath: String): Seq[String] =
    val src = Source.fromFile(new File(filePath))
    val fileContents = src.getLines().mkString("\n")
    val result = fileContents.split(";").map(_.trim).filter(_.nonEmpty).toSeq
    src.close()
    result

  def getCSVFiles(dPath: String): Seq[Path] =
    val directoryPath = s"$dPath/$currentData"
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

  def loadCSV[T](filePath: Path, rowToInstance: Seq[String] => T): Seq[T] =
    val file = filePath.toFile
    val source = Source.fromFile(file)

    try {
      val lines = source.getLines().drop(1).toSeq

      lines.map { line =>
        val cols = line.split(",").map(_.trim).toSeq
        rowToInstance(cols)
      }
    } finally {
      source.close()
    }

  def resultSetToCSV(resultSet: ResultSet, outputFile: String): Unit =
    if (resultSet == null) // skipped test
//      println(s"SKIPPING result set $outputFile")
      {}
    else
      val writer = new BufferedWriter(new FileWriter(outputFile))

      try {
        val metaData: ResultSetMetaData = resultSet.getMetaData
        val columnCount: Int = metaData.getColumnCount

        val header = (1 to columnCount).map(metaData.getColumnName).mkString(",")
        writer.write(header)
        writer.newLine()

        while (resultSet.next()) {
          val row = (1 to columnCount).map { i =>
            val value = resultSet.getObject(i)
            if (value != null) value.toString else "" // Handle null values
          }.mkString(",")

          writer.write(row)
          writer.newLine()
        }
      } finally {
        writer.flush()
        writer.close()
      }

  def collectionToCSV[T](data: Seq[T], outputFile: String, headers: Seq[String], toCsvRow: T => Seq[String]): Unit =
    if (data == null) // skipped test
      {}
    else
      val file = new BufferedWriter(new FileWriter(outputFile))

      try {
        file.write(headers.mkString(","))
        file.newLine()

        data.foreach { row =>
          file.write(toCsvRow(row).mkString(","))
          file.newLine()
        }
      } finally {
        file.flush()
        file.close()
      }

  def deleteOutputFiles(p: String, fileName: String): Unit =
    val filePath = s"$p/$fileName.csv"
    val path = Paths.get(filePath)
    if (Files.exists(path)) {
      try {
        Files.delete(path)
      } catch {
        case e: Exception => println(s"Failed to delete file: ${e.getMessage}")
      }
    }
}