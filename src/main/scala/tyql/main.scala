package tyql

import language.experimental.namedTuples
import scala.language.implicitConversions
import NamedTuple.{AnyNamedTuple, NamedTuple}
import scala.io.Source
import java.io.File
import buildinfo.BuildInfo
//import tyql.fix.FixedPointQuery.fix
//
//type Edge = (x: Int, y: Int)
//type GraphDB = (edge: Edge)
//
//val testDB = (tables = (
//  edge = Seq[Edge](
//    (x = 0, y = 1),
//    (x = 1, y = 2),
//    (x = 2, y = 3)
//  )
//))
//

def readDDLFile(filePath: String): Seq[String] = {
  val src = Source.fromFile(new File(filePath))
  val fileContents = src.getLines().mkString("\n")
  val result = fileContents.split(";").map(_.trim).filter(_.nonEmpty).toSeq
  src.close()
  result
}

@main def main() =
  import java.sql.{Connection, DriverManager, ResultSet}

  Class.forName("org.duckdb.DuckDBDriver")

  val connection: Connection = DriverManager.getConnection("jdbc:duckdb:")

  try {
    val ddl = s"${BuildInfo.baseDirectory}/bench/data/tc/schema.ddl"
    val ddlCmds = readDDLFile(ddl)
    val statement = connection.createStatement()

    ddlCmds.foreach(ddl =>
      println(s"Executing DDL: $ddl")
      statement.execute(ddl)
    )

    statement.execute(s"COPY tc_edge FROM '${BuildInfo.baseDirectory}/bench/data/tc/edge.csv'")

    val resultSet: ResultSet = statement.executeQuery("SELECT * FROM tc_edge")

    println("Query Results:")
    while (resultSet.next()) {
      val x = resultSet.getInt("x")
      val y = resultSet.getInt("y")
      println(s"x: $x, y: $y")
    }

  } finally {
    connection.close()
  }

//  val path = testDB.tables.edge
//  val result = fix(path, Seq())(path =>
//    path.flatMap(p =>
//      testDB.tables.edge
//        .filter(e => p.y == e.x)
//        .map(e => (x = p.x, y = e.y))
//    ).distinct
//  )//.filter(p => p.x > 1).map(p => p.x)
//
//  println(s"fix=$result")