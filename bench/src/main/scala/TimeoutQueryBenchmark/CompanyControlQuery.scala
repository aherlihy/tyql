package tyql.bench

import buildinfo.BuildInfo
import scalasql.PostgresDialect.*
import scalasql.core.SqlStr.SqlStringSyntax
import scalasql.{Expr, query, Table as ScalaSQLTable}

import java.sql.{Connection, ResultSet}
import scala.annotation.experimental
import scala.jdk.CollectionConverters.*
import scala.language.experimental.namedTuples
import scala.NamedTuple.*
import tyql.{Ord, Query, Table}
import tyql.Query.{fix, unrestrictedFix}
import tyql.Expr.{IntLit, StringLit, min, sum}
import Helpers.*

import tyql.Dialect.ansi.given

@experimental
class TOCompanyControlQuery extends QueryBenchmark {
  override def name = "cc"
  override def set = true
  if !set then ???

  // TYQL data model
  type Shares = (byC: String, of: String, percent: Int)
  type Control = (com1: String, com2: String)
  type CompanyControlDB = (shares: Shares, control: Control)
  val tyqlDB = (
    shares = Table[Shares]("cc_shares"),
    control = Table[Control]("cc_control")
  )

  // Collections data model + initialization
  case class SharesCC(byC: String, of: String, percent: Int)
  case class ResultCC(com1: String, com2: String)
  def toCollRow(row: Seq[String]): SharesCC = SharesCC(row(0), row(1), row(2).toInt)
  case class CollectionsDB(shares: Seq[SharesCC], control: Seq[ResultCC])
  def fromCollRes(r: ResultCC): Seq[String] = Seq(
    r.com1.toString,
    r.com2.toString
  )
  var collectionsDB: CollectionsDB = null

  def initializeCollections(): Unit =
    val allCSV = getCSVFiles(datadir)
    val tables = allCSV.map(s => (s.getFileName.toString.replace(".csv", ""), s)).map((name, csv) =>
      val loaded = name match
        case "shares" =>
          loadCSV(csv, toCollRow)
        case _ => ???
      (name, loaded)
    ).toMap
    collectionsDB = CollectionsDB(tables("shares"), Seq())

  //   ScalaSQL data model
  case class SharesSS[T[_]](byC: T[String], of: T[String], percent: T[Int])
  case class ResultSS[T[_]](com1: T[String], com2: T[String])
  def fromSSRes(r: ResultSS[?]): Seq[String] = Seq(
    r.com1.toString,
    r.com2.toString
  )

  object cc_shares extends ScalaSQLTable[SharesSS]
  object cc_control extends ScalaSQLTable[ResultSS]

  object cc_delta1 extends ScalaSQLTable[SharesSS]
  object cc_derived1 extends ScalaSQLTable[SharesSS]
  object cc_tmp1 extends ScalaSQLTable[SharesSS]
  object cc_delta2 extends ScalaSQLTable[ResultSS]
  object cc_derived2 extends ScalaSQLTable[ResultSS]
  object cc_tmp2 extends ScalaSQLTable[ResultSS]
  object cc_empty_shares extends ScalaSQLTable[SharesSS]

  // Result types for later printing
  var resultTyql: ResultSet = null
  var resultScalaSQL: Seq[ResultSS[?]] = null
  var resultCollections: Seq[ResultCC] = null
  var backupResultScalaSql: ResultSet = null

  // Execute queries
  def executeTyQL(ddb: DuckDBBackend): Unit =
    val (cshares, control) = unrestrictedFix(tyqlDB.shares, tyqlDB.control)((cshares, control) =>
      val csharesRecur = control.aggregate(con =>
        cshares
          .filter(cs => cs.byC == con.com2)
          .aggregate(cs => (byC = con.com1, of = cs.of, percent = sum(cs.percent)).toGroupingRow)
      ).groupBySource((con, csh) => (byC = con.com1, of = csh.of).toRow).distinct
      val controlRecur = cshares
        .filter(s => s.percent > 50)
        .map(s => (com1 = s.byC, com2 = s.of).toRow)
        .distinct
      (csharesRecur, controlRecur)
    )
    val query = control.sort(_.com1, Ord.ASC).sort(_.com2, Ord.ASC)

    val queryStr = query.toQueryIR.toSQLString()
    resultTyql = ddb.runQuery(queryStr)

  def executeCollections(): Unit =
    var it = 0

    val sharesBase = collectionsDB.shares
    val controlBase = collectionsDB.control

    val (shares, control) = FixedPointQuery.multiFix(set)((sharesBase, controlBase), (Seq(), Seq()))((recur, acc) =>
      if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
      val (cshares, ccontrol) = recur
      val (csharesAcc, controlAcc) = if it == 0 then (sharesBase, controlBase) else acc
      it += 1
      val csharesRecur = controlAcc.flatMap(con =>
        if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
        cshares
          .filter(cs =>
            if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
            cs.byC == con.com2
          )
          .map(cs =>
            if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
            SharesCC(con.com1, cs.of, cs.percent)
          )
          .groupBy(csh =>
            if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
            (csh.byC, csh.of)
          )
          .map((k, v) =>
            if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
            SharesCC(k._1, k._2, v.map(s3 => s3.percent).sum)
          )
          .toSeq
      )

      val controlRecur = csharesAcc
        .filter(s => s.percent > 50)
        .map(s => ResultCC(com1 = s.byC, com2 = s.of))
      (csharesRecur, controlRecur)
    )
    resultCollections = control.sortBy(_.com1).sortBy(_.com2)
    println(s"\nIT,$name,collections,$it")

  def executeScalaSQL(ddb: DuckDBBackend): Unit =
    var it = 0
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection

    val initBase = () =>
      (cc_shares.select.map(c => (c.byC, c.of, c.percent)), cc_control.select.map(c => (c.com1, c.com2)))

    def gt50(v1: Expr[Int]): Expr[Boolean] = Expr { implicit ctx => sql"$v1 > 50" }
    val fixFn: ((ScalaSQLTable[SharesSS], ScalaSQLTable[ResultSS])) => (String, String) =
      recur =>
        val (cshares, control) = recur
        val (csharesAcc, controlAcc) = if it == 0 then (cc_delta1, cc_delta2) else (cc_derived1, cc_derived2)
        it += 1
        val csharesRecur = "SELECT ref22.com1 as byC, ref23.of as of, SUM(ref23.percent) as percent " +
          s"FROM ${ScalaSQLTable.name(controlAcc)} as ref22, ${ScalaSQLTable.name(cshares)} as ref23 " +
          "WHERE ref23.byC = ref22.com2 " +
          "GROUP BY ref22.com1, ref23.of "
//        )
//        val csharesRecur = if (fixAgg.isEmpty) // workaround scalasql doesn't allow empty values
//          cc_empty_shares.select.map(c => (c.byC, c.of, c.percent))
//        else
//          db.values(fixAgg)

        val controlRecur = s"SELECT ref24.byC as com1, ref24.of as com2 " +
          s"FROM ${ScalaSQLTable.name(csharesAcc)} as ref24 " +
          s"WHERE ref24.percent > 50"
//          csharesAcc.select
//          .filter(s => gt50(s.percent))
//          .map(s =>(s.byC, s.of))

        (csharesRecur, controlRecur)

    FixedPointQuery.agg_scalaSQLSemiNaiveTWO(set)(
      ddb,
      (cc_delta1, cc_delta2),
      (cc_tmp1, cc_tmp2),
      (cc_derived1, cc_derived2)
    )(
      ((c: SharesSS[?]) => (c.byC, c.of, c.percent), (c: ResultSS[?]) => (c.com1, c.com2))
    )(
      initBase.asInstanceOf[() => (query.Select[Any, Any], query.Select[Any, Any])]
    )(fixFn)

    val result = cc_derived2.select.sortBy(_.com1).sortBy(_.com2)
    resultScalaSQL = db.run(result)
    println(s"\nIT,$name,scalasql,$it")

  // Write results to csv for checking
  def writeTyQLResult(): Unit =
    val outfile = s"$outdir/tyql.csv"
    resultSetToCSV(resultTyql, outfile)

  def writeCollectionsResult(): Unit =
    val outfile = s"$outdir/collections.csv"
    collectionToCSV(resultCollections, outfile, Seq("com1", "com2"), fromCollRes)

  def writeScalaSQLResult(): Unit =
    val outfile = s"$outdir/scalasql.csv"
    if (backupResultScalaSql != null)
      resultSetToCSV(backupResultScalaSql, outfile)
    else
      collectionToCSV(resultScalaSQL, outfile, Seq("com1", "com2"), fromSSRes)

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
