package tyql.bench

import buildinfo.BuildInfo
import scalasql.MySqlDialect.*
import scalasql.core.SqlStr.SqlStringSyntax
import scalasql.{Expr, Table as ScalaSQLTable}
import Helpers.*

import java.sql.{Connection, ResultSet}
import scala.annotation.experimental
import scala.jdk.CollectionConverters.*
import scala.language.experimental.namedTuples
import scala.NamedTuple.*
import tyql.{Ord, Table}
import tyql.Expr.{IntLit, min}

@experimental
class AncestryQuery extends QueryBenchmark {
  override def name = "ancestry"

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
  case class GenSS[T[_]](name: T[String], gen: T[Int])
  case class ResultSS[T[_]](name: T[String])

  def fromSSRes(r: ResultSS[?]): Seq[String] = Seq(
    r.name.toString
  )

  object ancestry_parents extends ScalaSQLTable[ParentSS]
  object ancestry_base extends ScalaSQLTable[GenSS]
  object ancestry_recur extends ScalaSQLTable[GenSS]

  // Result types for later printing
  var resultTyql: ResultSet = null
  var resultScalaSQL: Seq[String] = null
  var resultCollections: Seq[ResultCC] = null
  var backupResultScalaSql: ResultSet = null

  // Execute queries
  def executeTyQL(ddb: DuckDBBackend): Unit =
    val base = tyqlDB.parents.filter(p => p.parent == "Alice").map(e => (name = e.child, gen = IntLit(1)).toRow)
    val query = base.fix(gen =>
      tyqlDB.parents.flatMap(parent =>
        gen
          .filter(g => parent.parent == g.name)
          .map(g => (name = parent.child, gen = g.gen + 1).toRow)
      ).distinct
    ).filter(g => g.gen == 2).map(g => (name = g.name).toRow).sort(_.name, Ord.ASC)

    val queryStr = query.toQueryIR.toSQLString().replace("\"", "'")
    resultTyql = ddb.runQuery(queryStr)

  def executeCollections(): Unit =
    val base = collectionsDB.parents.filter(p => p.parent == "Alice").map(e => GenCC(name = e.child, gen = 1))
    resultCollections = FixedPointQuery.fix(base, Seq())(sp =>
        collectionsDB.parents.flatMap(parent =>
          sp
            .filter(g => parent.parent == g.name)
            .map(g => GenCC(name = parent.child, gen = g.gen + 1))
        ).distinct
    ).filter(g => g.gen == 2).map(g => ResultCC(name = g.name)).sortBy(_.name)


  def executeScalaSQL(ddb: DuckDBBackend): Unit =
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection
    val dropTable = ancestry_recur.delete(_ => true)
    db.run(dropTable)

    val base = ancestry_base.insert.select(
      c => (c.name, c.gen),
      ancestry_parents.select
        .filter(p => p.parent === "Alice").map(e => (e.child, Expr(1)))
    )
    db.run(base)

    val fixFn: () => Unit = () =>
      val innerQ = for {
        base <- ancestry_base.select
        parents <- ancestry_parents.join(base.name === _.parent)
      } yield (parents.child, base.gen + 1)

      val query = ancestry_recur.insert.select(
        c => (c.name, c.gen),
        innerQ
      )
      db.run(query)

    val cmp: () => Boolean = () =>
      val diff = ancestry_recur.select.except(ancestry_base.select)
      val delta = db.run(diff)
      delta.isEmpty

    val reInit: () => Unit = () =>
      // for set-semantic insert delta
      val delta = ancestry_recur.select.except(ancestry_base.select).map(r => (r.name, r.gen))

      val insertNew = ancestry_base.insert.select(
        c => (c.name, c.gen),
        delta
      )
      db.run(insertNew)
      db.run(ancestry_recur.delete(_ => true))

    FixedPointQuery.dbFix(ancestry_base, ancestry_recur)(fixFn)(cmp)(reInit)
    val result = ancestry_base.select
      .filter(_.gen === 2)
      .sortBy(_.name)
      .map(r => r.name)
    resultScalaSQL = db.run(result)

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
