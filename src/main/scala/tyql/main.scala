package tyql

import language.experimental.namedTuples
import scala.language.implicitConversions
import NamedTuple.{AnyNamedTuple, NamedTuple}
import scala.io.Source
import java.io.File
import buildinfo.BuildInfo
import scalasql.Table as ScalaSQLTable
import scalasql.PostgresDialect._
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

case class EdgeSS[T[_]](x: T[Int], y: T[Int])

case class ResultEdgeSS[T[_]](startNode: T[Int], endNode: T[Int], path: T[Seq[Int]])

object Edge extends ScalaSQLTable[EdgeSS]

//@main def main() =
//  import java.sql.{Connection, DriverManager, ResultSet}
//
//  Class.forName("org.duckdb.DuckDBDriver")
//
//  val connection: Connection = DriverManager.getConnection("jdbc:duckdb:")
//
//
//  try {
//    val ddl = s"${BuildInfo.baseDirectory}/bench/data/tc/schema.ddl"
//    val ddlCmds = readDDLFile(ddl)
//    val statement = connection.createStatement()
//
//    ddlCmds.foreach(ddl =>
//      println(s"Executing DDL: $ddl")
//      statement.execute(ddl)
//    )
//
//    statement.execute(s"COPY tc_edge FROM '${BuildInfo.baseDirectory}/bench/data/tc/edge.csv'")
//
//    val resultSet: ResultSet = statement.executeQuery("SELECT * FROM tc_edge")
//
//    println("Query Results:")
//    while (resultSet.next()) {
//      val x = resultSet.getInt("x")
//      val y = resultSet.getInt("y")
//      println(s"x: $x, y: $y")
//    }
//    val dbClient = scalasql.DbClient.Connection(connection, new scalasql.Config {
//      override def tableNameMapper(v: String) = s"tc_${v.toLowerCase()}"
//    })
//    val db = dbClient.getAutoCommitClientConnection
//    val query = Edge.select
//    println(s"ScalaSQL query=${db.renderSql(query)}")
//    val res = db.run(query)
//    println(s"ScalaSQL result=$res")
//
//
//  } finally {
//    connection.close()
//  }

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

// works with explicit type params
trait MyBehavior2[A, B]:
  def apply(a: A, b: B): String

trait LowPriorityMyBehavior:
  given general[A, B](using A =:= false): MyBehavior2[A, B] with
    def apply(a: A, b: B) = "Less specific"

object MyBehavior2 extends LowPriorityMyBehavior:
  given specific[A, B](using A =:= true): MyBehavior2[A, B] with
    def apply(a: A, b: B) = "More specific"

def f2[A, B](a: A, b: B)(using ev: MyBehavior2[A, B]): String = ev(a, b)

//@main def main() =
//  val a: true = true
//  val a2: false = false
//  val b: Int = 42
//  println(f2[false, Int](a2, b))

class AT
class BT
class CT

trait MyBehavior3[A, B]:
  def apply(a: A, b: B): String

object MyBehavior3:
//  given general[A, B](using A =:= AT): MyBehavior3[A, B] with
//    def apply(a: A, b: B) = "AT"
  given specific1[A, B](using A =:= BT): MyBehavior3[A, B] with
    def apply(a: A, b: B) = "BT"
  given specific2[A, B](using A =:= CT): MyBehavior3[A, B] with
    def apply(a: A, b: B) = "CT"

  def f3[A, B](a: A, b: B)(using ev: MyBehavior3[A, B]): String = ev(a, b)

@main def main() =
  val a = AT()
  val b = BT()
  val c = CT()
  println(MyBehavior3.f3(b, b))
