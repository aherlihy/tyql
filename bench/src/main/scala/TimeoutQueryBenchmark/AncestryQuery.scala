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
  private val outputHeader = Seq("name")
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
  var resultJDBC_RSQL: ResultSet = null
  var resultJDBC_SNE: ResultSet = null
  var resultScalaSQL: Seq[String] = null
  var resultCollections: Seq[ResultCC] = null
  var backupResultScalaSql: ResultSet = null

  // Execute queries
  def executeJDBC_RSQL(ddb: DuckDBBackend): Unit =
    val queryStr =
      "WITH RECURSIVE recursive1 AS ((SELECT ancestry_parents1.child as name, 1 as gen FROM ancestry_parents as ancestry_parents1 WHERE ancestry_parents1.parent = '1') UNION ((SELECT ancestry_parents3.child as name, ref3.gen + 1 as gen FROM ancestry_parents as ancestry_parents3, recursive1 as ref3 WHERE ancestry_parents3.parent = ref3.name)))\n SELECT recref0.name as name FROM recursive1 as recref0 WHERE recref0.gen = 2 ORDER BY name ASC"
    resultJDBC_RSQL = ddb.runQuery(queryStr)

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
  def writeBenchResult(mode: QueryMode): Unit =
    mode match
      case QueryMode.TyQL =>
        val outfile = s"$outdir/tyql.csv"
        resultSetToCSV(resultTyql, outfile)
      case QueryMode.ScalaSQL =>
        val outfile = s"$outdir/scalasql.csv"
        if (backupResultScalaSql != null)
          resultSetToCSV(backupResultScalaSql, outfile)
        else
          collectionToCSV(resultScalaSQL, outfile, outputHeader, t => Seq(t))
      case QueryMode.Collections =>
        val outfile = s"$outdir/collections.csv"
        collectionToCSV(resultCollections, outfile, outputHeader, fromCollRes)
      case QueryMode.JDBC_SNE =>
        val outfile = s"$outdir/jdbc_sne.csv"
        resultSetToCSV(resultJDBC_SNE, outfile)
      case QueryMode.JDBC_RSQL =>
        val outfile = s"$outdir/jdbc_sne.csv"
        resultSetToCSV(resultJDBC_RSQL, outfile)

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
