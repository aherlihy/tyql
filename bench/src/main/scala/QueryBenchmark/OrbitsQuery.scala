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
import tyql.{Ord, Table, Query}
import tyql.Expr.max

@experimental
class OrbitsQuery extends QueryBenchmark {
  override def name = "orbits"
  override def set = true

  // TYQL data model
  type Orbits = (x: String, y: String)
  type PlanetaryDB = (base: Orbits)
  val tyqlDB = (
    base = Table[Orbits]("orbits_base"),
  )

  // Collections data model + initialization
  case class OrbitsCC(x: String, y: String)
  def toCollRow(row: Seq[String]): OrbitsCC = OrbitsCC(row(0).toString, row(1).toString)
  case class CollectionsDB(base: Seq[OrbitsCC])
  def fromCollRes(r: OrbitsCC): Seq[String] = Seq(
    r.x.toString,
    r.y.toString,
  )
  var collectionsDB: CollectionsDB = null

  def initializeCollections(): Unit =
    val allCSV = getCSVFiles(datadir)
    val tables = allCSV.map(s => (s.getFileName.toString.replace(".csv", ""), s)).map((name, csv) =>
      val loaded = name match
        case "base" =>
          loadCSV(csv, toCollRow)
        case _ => ???
      (name, loaded)
    ).toMap
    collectionsDB = CollectionsDB(tables("base"))

  //   ScalaSQL data model
  case class OrbitsSS[T[_]](x: T[String], y: T[String])

  def fromSSRes(r: OrbitsSS[?]): Seq[String] = Seq(
    r.x.toString,
    r.y.toString
  )

  object orbits_base extends ScalaSQLTable[OrbitsSS]
  object orbits_delta extends ScalaSQLTable[OrbitsSS]
  object orbits_derived extends ScalaSQLTable[OrbitsSS]
  object orbits_tmp extends ScalaSQLTable[OrbitsSS]
  //

  // Result types for later printing
  var resultTyql: ResultSet = null
  var resultScalaSQL: Seq[OrbitsSS[?]] = null
  var resultCollections: Seq[OrbitsCC] = null
  var backupResultScalaSql: ResultSet = null

  // Execute queries
  def executeTyQL(ddb: DuckDBBackend): Unit =
    val base = tyqlDB.base
    val orbits =
      if (set)
        base.unrestrictedBagFix(orbits =>
          orbits.flatMap(p =>
            orbits
              .filter(e => p.y == e.x)
              .map(e => (x = p.x, y = e.y).toRow)
          )
        )
      else
        base.unrestrictedFix(orbits =>
          orbits.flatMap(p =>
            orbits
              .filter(e => p.y == e.x)
              .map(e => (x = p.x, y = e.y).toRow)
          )
        )

    val query = orbits match
      case Query.MultiRecursive(_, _, orbitsRef) =>
        orbits.filter(o =>
          orbitsRef
            .flatMap(o1 =>
              orbitsRef
                .filter(o2 => o1.y == o2.x)
                .map(o2 => (x = o1.x, y = o2.y).toRow)
            )
            .filter(io => o.x == io.x && o.y == io.y)
            .nonEmpty
        ).sort(_.x, Ord.ASC).sort(_.y, Ord.ASC)

    val queryStr = query.toQueryIR.toSQLString()
    resultTyql = ddb.runQuery(queryStr)

  def executeCollections(): Unit =
    val base = collectionsDB.base
    val orbits = FixedPointQuery.fix(set)(base, Seq())(orbits =>
      orbits.flatMap(p =>
        orbits
          .filter(e => p.y == e.x)
          .map(e => OrbitsCC(x = p.x, y = e.y))
      )
    )
    resultCollections = orbits.filter(o0 =>
      orbits.exists(o4 =>
        orbits.exists(o5 =>
          o4.y == o5.x && o0.x == o4.x && o0.y == o5.y
        )
      )
    ).sortBy(_.x).sortBy(_.y)

  def executeScalaSQL(ddb: DuckDBBackend): Unit =
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection
    val toTuple = (c: OrbitsSS[?]) => (c.x, c.y)

    val initBase = () => orbits_base.select.map(o => (o.x, o.y))

    val fixFn: ScalaSQLTable[OrbitsSS] => query.Select[(Expr[String], Expr[String]), (String, String)] = orbits =>
      for {
        p <- orbits.select
        e <- orbits.join(p.y === _.x)
      } yield (p.x, e.y)

    FixedPointQuery.scalaSQLSemiNaive(set)(
      ddb,
      orbits_delta,
      orbits_tmp,
      orbits_derived
    )(toTuple)(initBase.asInstanceOf[() => query.Select[Any, Any]])(
      fixFn.asInstanceOf[ScalaSQLTable[OrbitsSS] => query.Select[Any, Any]]
    )

    //    orbits_base.select.groupBy(_.dst)(_.dst) groupBy does not work with ScalaSQL + postgres
    backupResultScalaSql = ddb.runQuery(
      "SELECT *" +
        s"FROM ${ScalaSQLTable.name(orbits_derived)} as recref0" +
        " WHERE EXISTS" +
        "     (SELECT * FROM" +
        "     (SELECT ref4.x as x, ref5.y as y" +
        s"        FROM ${ScalaSQLTable.name(orbits_derived)} as ref4, ${ScalaSQLTable.name(orbits_derived)} as ref5" +
        "       WHERE ref4.y = ref5.x) as subquery9" +
        "     WHERE recref0.x = subquery9.x AND recref0.y = subquery9.y)" +
        " ORDER BY recref0.y, recref0.x"
    )

  // Write results to csv for checking
  def writeTyQLResult(): Unit =
    val outfile = s"$outdir/tyql.csv"
    resultSetToCSV(resultTyql, outfile)

  def writeCollectionsResult(): Unit =
    val outfile = s"$outdir/collections.csv"
    collectionToCSV(resultCollections, outfile, Seq("x", "y"), fromCollRes)

  def writeScalaSQLResult(): Unit =
    val outfile = s"$outdir/scalasql.csv"
    if (backupResultScalaSql != null)
      resultSetToCSV(backupResultScalaSql, outfile)
    else
      collectionToCSV(resultScalaSQL, outfile, Seq("x", "y"), fromSSRes)

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
