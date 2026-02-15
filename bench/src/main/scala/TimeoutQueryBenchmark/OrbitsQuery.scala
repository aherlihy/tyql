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
import scala.collection.mutable.ArrayBuffer
import tyql.{Ord, Table, Query}
import tyql.Expr.max

@experimental
class TOOrbitsQuery extends QueryBenchmark {
  override def name = "orbits"
  private val outputHeader = Seq("x", "y")
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
  var resultJDBC_RSQL: ResultSet = null
  var resultJDBC_SNE: ResultSet = null
  var resultScalaSQL: Seq[OrbitsSS[?]] = null
  var resultCollections: Seq[OrbitsCC] = null
  var backupResultScalaSql: ResultSet = null

  // Execute queries
  def executeJDBC_RSQL(ddb: DuckDBBackend): Unit =
    val queryStr =
      "WITH RECURSIVE recursive1 AS ((SELECT * FROM orbits_base as orbits_base1) UNION ALL ((SELECT ref0.x as x, ref1.y as y FROM recursive1 as ref0, recursive1 as ref1 WHERE ref0.y = ref1.x)))\n SELECT * FROM recursive1 as recref0 WHERE EXISTS (SELECT * FROM (SELECT ref4.x as x, ref5.y as y FROM recursive1 as ref4, recursive1 as ref5 WHERE ref4.y = ref5.x) as subquery9 WHERE recref0.x = subquery9.x AND recref0.y = subquery9.y) ORDER BY y ASC, x ASC"
    resultJDBC_RSQL = ddb.runQuery(queryStr)

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
                .map(o2 => (x = o1.x, y = o2.y).toRow))
            .filter(io => o.x == io.x && o.y == io.y)
            .nonEmpty
        ).sort(_.x, Ord.ASC).sort(_.y, Ord.ASC)

    val queryStr = query.toQueryIR.toSQLString()
    resultTyql = ddb.runQuery(queryStr)

  def executeCollections(): Unit =
    var it = 0
    val base = collectionsDB.base
    val orbits = FixedPointQuery.fix(set)(base, Seq())(orbits =>
      it += 1
        orbits.flatMap(p =>
          if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
          orbits
           .filter(e =>
             if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
             p.y == e.x)
           .map(e =>
             if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
             OrbitsCC(x = p.x, y = e.y))
        )
      )
    resultCollections = orbits.filter(o0 =>
      if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
      orbits.exists(o4 =>
        if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
        orbits.exists(o5 =>
          if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
          o4.y == o5.x && o0.x == o4.x && o0.y == o5.y
        )
      )
    ).sortBy(_.x).sortBy(_.y)
    println(s"\nIT,$name,collections,$it")


  def executeScalaSQL(ddb: DuckDBBackend): Unit =
    var it = 0
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection
    val toTuple = (c: OrbitsSS[?]) => (c.x, c.y)

    val initBase = () => orbits_base.select.map(o => (o.x, o.y))

    val fixFn: ScalaSQLTable[OrbitsSS] => query.Select[(Expr[String], Expr[String]), (String, String)] = orbits =>
      it += 1
      for {
        p <- orbits.select
        e <- orbits.join(p.y === _.x)
      } yield (p.x, e.y)

    FixedPointQuery.scalaSQLSemiNaive(set)(
      ddb, orbits_delta, orbits_tmp, orbits_derived
    )(toTuple)(initBase.asInstanceOf[() => query.Select[Any, Any]])(fixFn.asInstanceOf[ScalaSQLTable[OrbitsSS] => query.Select[Any, Any]])

    printResultSet(ddb.runQuery("SELECT * FROM orbits_derived"), "FINAL ScalaSQL")
    //    orbits_derived.select.groupBy(_.dst)(_.dst) groupBy does not work with ScalaSQL + postgres
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
    println(s"\nIT,$name,scalasql,$it")

  override def executeJDBC_SNE(ddb: DuckDBBackend): Unit =
    var it = 0

    val initBase = () => s"SELECT orbits_base0.x, orbits_base0.y FROM ${ScalaSQLTable.name(orbits_base)} orbits_base0"

    val linearFixFn = (orbits: String, _: String) =>
      it += 1
      s"SELECT orbits_delta0.x AS x, orbits_delta1.y AS y FROM $orbits orbits_delta0 JOIN $orbits orbits_delta1 ON (orbits_delta0.y = orbits_delta1.x)"

    val nonlinearFixFn = (delta: String, derived: String) =>
      it += 1
      val sq1 = s"SELECT orbits0.x AS x, orbits1.y AS y FROM $delta orbits0 JOIN $derived orbits1 ON (orbits0.y = orbits1.x)"
      val sq2 = s"SELECT orbits0.x AS x, orbits1.y AS y FROM $derived orbits0 JOIN $delta orbits1 ON (orbits0.y = orbits1.x)"
      s"$sq1 UNION $sq2"

    JDBC_SNEFixedPointQuery.jdbcSemiNaiveIteration(set)(
      ddb, ScalaSQLTable.name(orbits_delta), ScalaSQLTable.name(orbits_tmp), ScalaSQLTable.name(orbits_derived)
    )(initBase)(nonlinearFixFn)

    resultJDBC_SNE = ddb.runQuery(
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
    printResultSet(ddb.runQuery("SELECT * FROM orbits_derived"), "FINAL JDBC_SNE")
    println(s"\nIT,$name,jdbc,$it")

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
          collectionToCSV(resultScalaSQL, outfile, outputHeader, fromSSRes)
      case QueryMode.Collections =>
        val outfile = s"$outdir/collections.csv"
        collectionToCSV(resultCollections, outfile, outputHeader, fromCollRes)
      case QueryMode.JDBC_SNE =>
        val outfile = s"$outdir/jdbc_sne.csv"
        resultSetToCSV(resultJDBC_SNE, outfile)
      case QueryMode.JDBC_RSQL =>
        val outfile = s"$outdir/jdbc_sne.csv"
        resultSetToCSV(resultJDBC_RSQL, outfile)

  def JDBCToNT(resultSet: ResultSet): Seq[Orbits] =
    val buffer = ArrayBuffer.empty[Orbits]
    while (resultSet.next()) {
      val xV = resultSet.getString("x")
      val yV = resultSet.getString("y")
      buffer.append((x = xV, y = yV))
    }
    buffer.toSeq
}
