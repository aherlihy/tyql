package tyql.bench

import buildinfo.BuildInfo
import scalasql.PostgresDialect.*
import scalasql.core.SqlStr.SqlStringSyntax
import scalasql.{Expr, query, Table as ScalaSQLTable}
import Helpers.*

import java.sql.{Connection, ResultSet}
import scala.annotation.experimental
import scala.jdk.CollectionConverters.*
import scala.language.experimental.namedTuples
import scala.NamedTuple.*
import tyql.{Ord, Table}
import tyql.Expr.{IntLit, min}

@experimental
class TOAncestryQuery extends QueryBenchmark {
  override def name = "ancestry"
  val parentName = "1"
  override def set = true
  if !set then ???

  // TYQL data model
  type Parent = (parent: String, child: String)
  type AncestryDB = (parents: Parent)
  val tyqlDB = (
    parents = Table[Parent]("ancestry_parents")
    )

  // Collections data model + initialization
  case class ParentCC(parent: String, child: String)
  case class GenCC(name: String, gen: Int)
  case class ResultCC(name: String)
  def toCollRow(row: Seq[String]): ParentCC = ParentCC(row(0).toString, row(1).toString)
  case class CollectionsDB(parents: Seq[ParentCC])
  def fromCollRes(r: ResultCC): Seq[String] = Seq(
    r.name.toString
  )
  var collectionsDB: CollectionsDB = null

  def initializeCollections(): Unit =
    val allCSV = getCSVFiles(datadir)
    val tables = allCSV.map(s => (s.getFileName.toString.replace(".csv", ""), s)).map((name, csv) =>
      val loaded = name match
        case "parents" =>
          loadCSV(csv, toCollRow)
        case _ => ???
      (name, loaded)
    ).toMap
    collectionsDB = CollectionsDB(tables("parents"))

  //   ScalaSQL data model
  case class ParentSS[T[_]](parent: T[String], child: T[String])
  case class ResultSS[T[_]](name: T[String], gen: T[Int])

  object ancestry_parents extends ScalaSQLTable[ParentSS]
  object ancestry_delta extends ScalaSQLTable[ResultSS]
  object ancestry_derived extends ScalaSQLTable[ResultSS]
  object ancestry_tmp extends ScalaSQLTable[ResultSS]

  // Result types for later printing
  var resultTyql: ResultSet = null
  var resultScalaSQL: Seq[String] = null
  var resultCollections: Seq[ResultCC] = null
  var backupResultScalaSql: ResultSet = null

  // Execute queries
  def executeTyQL(ddb: DuckDBBackend): Unit =
    val base = tyqlDB.parents.filter(p => p.parent == parentName).map(e => (name = e.child, gen = IntLit(1)).toRow)
    val query = base.fix(gen =>
      tyqlDB.parents.flatMap(parent =>
        gen
          .filter(g => parent.parent == g.name)
          .map(g => (name = parent.child, gen = g.gen + 1).toRow)
      ).distinct
    ).filter(g => g.gen == 2)
      .map(g => (name = g.name).toRow)
      .sort(_.name, Ord.ASC)

    val queryStr = query.toQueryIR.toSQLString().replace("\"", "'")
    resultTyql = ddb.runQuery(queryStr)

  def executeCollections(): Unit =
    var it = 0
    val base = collectionsDB.parents.filter(p => p.parent == parentName).map(e => GenCC(name = e.child, gen = 1))
    resultCollections = FixedPointQuery.fix(set)(base, Seq())(sp =>
      it += 1
        collectionsDB.parents.flatMap(parent =>
          if (Thread.currentThread().isInterrupted) throw new Exception(s"$name timed out")
          sp
            .filter(g =>
              if (Thread.currentThread().isInterrupted) throw new Exception(s"$name timed out")
              parent.parent == g.name)
            .map(g =>
              if (Thread.currentThread().isInterrupted) throw new Exception(s"$name timed out")
              GenCC(name = parent.child, gen = g.gen + 1))
        ).distinct
    ).filter(g => g.gen == 2).map(g => ResultCC(name = g.name)).sortBy(_.name)
    println(s"\nIT,$name,collections,$it")

  def executeScalaSQL(ddb: DuckDBBackend): Unit =
    var it = 0
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection
    val toTuple = (c: ResultSS[?]) => (c.name, c.gen)
    def add1(v1: Expr[Int]): Expr[Int] = Expr { implicit ctx => sql"$v1 + 1" }
    def eqParent(v1: Expr[String]): Expr[Boolean] = Expr { implicit ctx => sql"$v1 = '1'" }
    def get1(): Expr[Int] = Expr { implicit ctx => sql"1" }

    val initBase = () =>
      ancestry_parents.select.filter(p => eqParent(p.parent)).map(e => (e.child, get1()))

    val fixFn: ScalaSQLTable[ResultSS] => query.Select[(Expr[String], Expr[Int]), (String, Int)] = parents =>
      it += 1
      for {
        base <- parents.select
        parents <- ancestry_parents.join(base.name === _.parent)
      } yield (parents.child, add1(base.gen))

    FixedPointQuery.scalaSQLSemiNaive(set)(
      ddb, ancestry_delta, ancestry_tmp, ancestry_derived
    )(toTuple)(initBase.asInstanceOf[() => query.Select[Any, Any]])(fixFn.asInstanceOf[ScalaSQLTable[ResultSS] => query.Select[Any, Any]])

    val result = ancestry_derived.select
      .filter(_.gen === 2)
      .sortBy(_.name)
      .map(r => r.name)
    resultScalaSQL = db.run(result)
    println(s"\nIT,$name,scalasql,$it")

  // Write results to csv for checking
  def writeTyQLResult(): Unit =
    val outfile = s"$outdir/tyql.csv"
    resultSetToCSV(resultTyql, outfile)

  def writeCollectionsResult(): Unit =
    val outfile = s"$outdir/collections.csv"
    collectionToCSV(resultCollections, outfile, Seq("name"), fromCollRes)

  def writeScalaSQLResult(): Unit =
    val outfile = s"$outdir/scalasql.csv"
    if (backupResultScalaSql != null)
      resultSetToCSV(backupResultScalaSql, outfile)
    else
      collectionToCSV(resultScalaSQL, outfile, Seq("name"), t => Seq(t))

  // Extract all results to avoid lazy-loading
  //  def printResultJDBC(resultSet: ResultSet): Unit =
  //    println("Query Results:")
  //    while (resultSet.next()) {
  //      val x = resultSet.getInt("startNode")
  //      val y = resultSet.getInt("endNode")
  //      val z = resultSet.getArray("path")
  //      println(s"x: $x, y: $y, path=$z")
  //    }
}
