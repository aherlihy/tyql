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
import tyql.{Ord, Table, Query}
import tyql.Query.unrestrictedFix
import tyql.Expr.{IntLit, StringLit, min}
import Helpers.*

@experimental
class CSPAQuery extends QueryBenchmark {
  override def name = "cspa"
  override def set = true

  // TYQL data model
  type Location = (p1: Int, p2: Int)
  type CSPADB = (assign: Location, dereference: Location, empty: Location)

  val tyqlDB = (
    assign = Table[Location]("cspa_assign"),
    dereference = Table[Location]("cspa_dereference"),
    empty = Table[Location]("cspa_empty")
  )

  // Collections data model + initialization
  case class PairCC(p1: Int, p2: Int)
  def toCollRow(row: Seq[String]): PairCC = PairCC(row(0).toInt, row(1).toInt)
  case class CollectionsDB(assign: Seq[PairCC], dereference: Seq[PairCC])
  def fromCollRes(r: PairCC): Seq[String] = Seq(
    r.p1.toString,
    r.p2.toString
  )
  var collectionsDB: CollectionsDB = null

  def initializeCollections(): Unit =
    val allCSV = getCSVFiles(datadir)
    val tables = allCSV.map(s => (s.getFileName.toString.replace(".csv", ""), s)).map((name, csv) =>
      val loaded = name match
        case "dereference" =>
          loadCSV(csv, toCollRow)
        case "assign" =>
          loadCSV(csv, toCollRow)
        case _ => ???
      (name, loaded)
    ).toMap
    collectionsDB = CollectionsDB(tables("assign"), tables("dereference"))

  //   ScalaSQL data model
  case class PairSS[T[_]](p1: T[Int], p2: T[Int])
  def fromSSRes(r: PairSS[?]): Seq[String] = Seq(
    r.p1.toString,
    r.p2.toString
  )

  object cspa_dereference extends ScalaSQLTable[PairSS]
  object cspa_assign extends ScalaSQLTable[PairSS]
  object cspa_empty extends ScalaSQLTable[PairSS]
  object cspa_delta1 extends ScalaSQLTable[PairSS]
  object cspa_derived1 extends ScalaSQLTable[PairSS]
  object cspa_tmp1 extends ScalaSQLTable[PairSS]
  object cspa_delta2 extends ScalaSQLTable[PairSS]
  object cspa_derived2 extends ScalaSQLTable[PairSS]
  object cspa_tmp2 extends ScalaSQLTable[PairSS]
  object cspa_delta3 extends ScalaSQLTable[PairSS]
  object cspa_derived3 extends ScalaSQLTable[PairSS]
  object cspa_tmp3 extends ScalaSQLTable[PairSS]

  // Result types for later printing
  var resultTyql: ResultSet = null
  var resultScalaSQL: Seq[PairSS[?]] = null
  var resultCollections: Seq[PairCC] = null
  var backupResultScalaSql: ResultSet = null

  // Execute queries
  def executeTyQL(ddb: DuckDBBackend): Unit =
    val assign = tyqlDB.assign
    val dereference = tyqlDB.dereference

    val memoryAliasBase =
      assign.map(a => (p1 = a.p2, p2 = a.p2).toRow)
        .union(
          assign.map(a => (p1 = a.p1, p2 = a.p1).toRow)
        )

    val valueFlowBase =
      assign
        .union(
          assign.map(a => (p1 = a.p1, p2 = a.p1).toRow)
        ).union(
          assign.map(a => (p1 = a.p2, p2 = a.p2).toRow)
        )

    val (valueFlowFinal, valueAliasFinal, memoryAliasFinal) = unrestrictedFix(valueFlowBase, tyqlDB.empty, memoryAliasBase)(
      (valueFlow, valueAlias, memoryAlias) =>
        val vfDef1 =
          for
            a <- assign
            m <- memoryAlias
            if a.p2 == m.p1
          yield (p1 = a.p1, p2 = m.p2).toRow
        val vfDef2 =
          for
            vf1 <- valueFlow
            vf2 <- valueFlow
            if vf1.p2 == vf2.p1
          yield (p1 = vf1.p1, p2 = vf2.p2).toRow
        val VF = vfDef1.union(vfDef2)

        val MA =
          for
            d1 <- dereference
            va <- valueAlias
            d2 <- dereference
            if d1.p1 == va.p1 && va.p2 == d2.p1
          yield (p1 = d1.p2, p2 = d2.p2).toRow

        val vaDef1 =
          for
            vf1 <- valueFlow
            vf2 <- valueFlow
            if vf1.p1 == vf2.p1
          yield (p1 = vf1.p2, p2 = vf2.p2).toRow
        val vaDef2 =
          for
            vf1 <- valueFlow
            m <- memoryAlias
            vf2 <- valueFlow
            if vf1.p1 == m.p1 && vf2.p1 == m.p2
          yield (p1 = vf1.p2, p2 = vf2.p2).toRow
        val VA = vaDef1.union(vaDef2)

        (VF, MA.distinct, VA)
    )

    val queryStr = valueFlowFinal.sort(_.p2, Ord.ASC).sort(_.p1, Ord.ASC).toQueryIR.toSQLString()
    resultTyql = ddb.runQuery(queryStr)

  def executeCollections(): Unit =

    val memoryAliasBase =
      collectionsDB.assign.map(a => PairCC(p1 = a.p2, p2 = a.p2))
        .union(
          collectionsDB.assign.map(a => PairCC(p1 = a.p1, p2 = a.p1))
        )

    val valueFlowBase =
      collectionsDB.assign
        .union(
          collectionsDB.assign.map(a => PairCC(p1 = a.p1, p2 = a.p1))
        ).union(
          collectionsDB.assign.map(a => PairCC(p1 = a.p2, p2 = a.p2))
        )

    var it = 0
    val (valueFlowFinal, valueAliasFinal, memoryAliasFinal) = FixedPointQuery.multiFix(true)((valueFlowBase, Seq[PairCC](), memoryAliasBase), (Seq(), Seq(), Seq()))(
      (recur, acc) => {
        val (valueFlow, valueAlias, memoryAlias) = recur
        val (valueFlowAcc, valueAliasAcc, memoryAliasAcc) = if it == 0 then (valueFlowBase, Seq[PairCC](), memoryAliasBase) else acc
        it += 1

        val valueFlowDef1 =
          for
            a <- collectionsDB.assign
            m <- memoryAliasAcc
            if a.p2 == m.p1
          yield PairCC(p1 = a.p1, p2 = m.p2)
        val valueFlowDef2 =
          for
            vf1 <- valueFlow
            vf2 <- valueFlow
            if vf1.p2 == vf2.p1
          yield PairCC(p1 = vf1.p1, p2 = vf2.p2)
        val VF = valueFlowDef1.union(valueFlowDef2)

        val memoryAliasDef =
          for
            d1 <- collectionsDB.dereference
            va <- valueAliasAcc
            d2 <- collectionsDB.dereference
            if d1.p1 == va.p1 && va.p2 == d2.p1
          yield PairCC(p1 = d1.p2, p2 = d2.p2)
        val MA = memoryAliasDef.distinct

        val valueAliasDef1 =
          for
            vf1 <- valueFlowAcc
            vf2 <- valueFlowAcc
            if vf1.p1 == vf2.p1
          yield PairCC(p1 = vf1.p2, p2 = vf2.p2)
        val valueAliasDef2 =
          for
            vf1 <- valueFlowAcc
            m <- memoryAliasAcc
            vf2 <- valueFlowAcc
            if vf1.p1 == m.p1 && vf2.p1 == m.p2
          yield PairCC(p1 = vf1.p2, p2 = vf2.p2)
        val VA = valueAliasDef1.union(valueAliasDef2)

        (VF, MA, VA)
      }
    )
    resultCollections = valueFlowFinal.sortBy(_.p2).sortBy(_.p1)


  def executeScalaSQL(ddb: DuckDBBackend): Unit =
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection
    val toTuple = (c: PairSS[?]) => (c.p1, c.p2)

    val initBase = () => {
      val memoryAliasBase = cspa_assign.select
        .map(a => (a.p2, a.p2))
        .union(
          cspa_assign.select
            .map(a => (a.p1, a.p1)))

      val valueFlowBase =
        cspa_assign.select.map(a => (a.p1, a.p2))
          .union(
            cspa_assign.select
              .map(a => (a.p1, a.p1)))
          .union(
            cspa_assign.select.map(a => (a.p2, a.p2)))
      (memoryAliasBase, cspa_empty.select.map(s => (s.p1, s.p2)), valueFlowBase)
    }

    var it = 0
    val fixFn: ((ScalaSQLTable[PairSS], ScalaSQLTable[PairSS], ScalaSQLTable[PairSS])) => (query.Select[(Expr[Int], Expr[Int]), (Int, Int)], query.Select[(Expr[Int], Expr[Int]), (Int, Int)], query.Select[(Expr[Int], Expr[Int]), (Int, Int)]) =
      recur => {
        val (valueFlow, valueAlias, memoryAlias) = recur
        val (valueFlowAcc, valueAliasAcc, memoryAliasAcc) = if it == 0 then (cspa_delta1, cspa_delta2, cspa_delta3) else (cspa_derived1, cspa_derived2, cspa_derived3)
        it += 1

        //        println(s"***iteration $it")
        //        println(s"BASES:\n\teven : ${db.runRaw[(String, String)](s"SELECT * FROM ${ScalaSQLTable.name(even)}").map(f => f._1 + "-" + f._2).mkString("(", ",", ")")}\n\todd: ${db.runRaw[(String, String)](s"SELECT * FROM ${ScalaSQLTable.name(odd)}").map(f => f._1 + "-" + f._2).mkString("(", ",", ")")}")
        //        println(s"DERIV:\n\teven : ${db.runRaw[(String, String)](s"SELECT * FROM ${ScalaSQLTable.name(derived_even)}").map(f => f._1 + "-" + f._2).mkString("(", ",", ")")}\n\todd: ${db.runRaw[(String, String)](s"SELECT * FROM ${ScalaSQLTable.name(derived_odd)}").map(f => f._1 + "-" + f._2).mkString("(", ",", ")")}")
        val valueFlowDef1 =
          for
            a <- cspa_assign.select
            m <- memoryAliasAcc.crossJoin()
            if a.p2 === m.p1
          yield (a.p1, m.p2)
        val valueFlowDef2 =
          for
            vf1 <- valueFlow.select
            vf2 <- valueFlow.crossJoin()
            if vf1.p2 === vf2.p1
          yield (vf1.p1, vf2.p2)
        val VF = valueFlowDef1.union(valueFlowDef2)

        val memoryAliasDef =
          for
            d1 <- cspa_dereference.select
            va <- valueAliasAcc.crossJoin()
            d2 <- cspa_dereference.crossJoin()
            if d1.p1 === va.p1 && va.p2 === d2.p1
          yield (d1.p2, d2.p2)
        val MA = memoryAliasDef.distinct

        val valueAliasDef1 =
          for
            vf1 <- valueFlowAcc.select
            vf2 <- valueFlowAcc.crossJoin()
            if vf1.p1 === vf2.p1
          yield (vf1.p2, vf2.p2)
        val valueAliasDef2 =
          for
            vf1 <- valueFlowAcc.select
            m <- memoryAliasAcc.crossJoin()
            vf2 <- valueFlowAcc.crossJoin()
            if vf1.p1 === m.p1 && vf2.p1 === m.p2
          yield (vf1.p2, vf2.p2)
        val VA = valueAliasDef1.union(valueAliasDef2)

        //        println(s"output:\n\teven: ${db.run(evenResult).map(f => f._1 + "-" + f._2).mkString("(", ",", ")")}\n\toddResult: ${db.run(oddResult).map(f => f._1 + "-" + f._2).mkString("(", ",", ")")}")
        (VF, MA, VA)
      }

    FixedPointQuery.scalaSQLSemiNaiveTHREE(set)(
      db, (cspa_delta1, cspa_delta2, cspa_delta3), (cspa_tmp1, cspa_tmp2, cspa_tmp3), (cspa_derived1, cspa_derived2, cspa_derived3)
    )(
      (toTuple, toTuple, toTuple)
    )(
      initBase.asInstanceOf[() => (query.Select[Any, Any], query.Select[Any, Any], query.Select[Any, Any])]
    )(fixFn.asInstanceOf[((ScalaSQLTable[PairSS], ScalaSQLTable[PairSS], ScalaSQLTable[PairSS])) => (query.Select[Any, Any], query.Select[Any, Any], query.Select[Any, Any])])

    val result = cspa_derived1.select.sortBy(_.p2).sortBy(_.p1)
    resultScalaSQL = db.run(result)


  // Write results to csv for checking
  def writeTyQLResult(): Unit =
    val outfile = s"$outdir/tyql.csv"
    resultSetToCSV(resultTyql, outfile)

  def writeCollectionsResult(): Unit =
    val outfile = s"$outdir/collections.csv"
    collectionToCSV(resultCollections, outfile, Seq("p1", "p1"), fromCollRes)

  def writeScalaSQLResult(): Unit =
    val outfile = s"$outdir/scalasql.csv"
    if (backupResultScalaSql != null)
      resultSetToCSV(backupResultScalaSql, outfile)
    else
      collectionToCSV(resultScalaSQL, outfile, Seq("p1", "p2"), fromSSRes)

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
