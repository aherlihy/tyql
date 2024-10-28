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
class DataflowQuery extends QueryBenchmark {
  override def name = "dataflow"
  override def set = true

  // TYQL data model
  type Instruction = (opN: String, varN: String)
  type Jump = (a: String, b: String)

  type FlowDB = (readOp: Instruction, writeOp: Instruction, jumpOp: Jump)

  val tyqlDB = (
      readOp = Table[Instruction]("dataflow_readOp"),
      writeOp = Table[Instruction]("dataflow_writeOp"),
      jumpOp = Table[Jump]("dataflow_jumpOp")
    )

  // Collections data model + initialization
  case class InstructionCC(opN: String, varN: String)
  case class JumpCC(a: String, b: String)
  case class ResultCC(r: String, w: String)
  def toCollRow1(row: Seq[String]): InstructionCC = InstructionCC(row(0).toString, row(1).toString)
  def toCollRow2(row: Seq[String]): JumpCC = JumpCC(row(0).toString, row(1).toString)
  case class CollectionsDB(readOp: Seq[InstructionCC], writeOp: Seq[InstructionCC], jumpOp: Seq[JumpCC])
  def fromCollRes(r: ResultCC): Seq[String] = Seq(
    r.r.toString,
    r.w.toString
  )
  var collectionsDB: CollectionsDB = null

  def initializeCollections(): Unit =
    val allCSV = getCSVFiles(datadir)
    val tables = allCSV.map(s => (s.getFileName.toString.replace(".csv", ""), s)).map((name, csv) =>
      val loaded = name match
        case "readOp" =>
          loadCSV(csv, toCollRow1)
        case "writeOp" =>
          loadCSV(csv, toCollRow1)
        case "jumpOp" =>
          loadCSV(csv, toCollRow2)
        case _ => ???
      (name, loaded)
    ).toMap
    collectionsDB = CollectionsDB(
      tables("readOp").asInstanceOf[Seq[InstructionCC]],
      tables("writeOp").asInstanceOf[Seq[InstructionCC]],
      tables("jumpOp").asInstanceOf[Seq[JumpCC]])

  //   ScalaSQL data model
  case class InstructionSS[T[_]](opN: T[String], varN: T[String])
  case class JumpSS[T[_]](a: T[String], b: T[String])
  case class ResultSS[T[_]](r: T[String], w: T[String])
  def fromResultSS(r: ResultSS[?]): Seq[String] = Seq(
    r.r.toString,
    r.w.toString
  )

  object dataflow_readOp extends ScalaSQLTable[InstructionSS]
  object dataflow_writeOp extends ScalaSQLTable[InstructionSS]
  object dataflow_jumpOp extends ScalaSQLTable[JumpSS]
  object dataflow_delta extends ScalaSQLTable[JumpSS]
  object dataflow_derived extends ScalaSQLTable[JumpSS]
  object dataflow_tmp extends ScalaSQLTable[JumpSS]

  // Result types for later printing
  var resultTyql: ResultSet = null
  var resultScalaSQL: Seq[ResultSS[?]] = null
  var resultCollections: Seq[ResultCC] = null
  var backupResultScalaSql: ResultSet = null

  // Execute queries
  def executeTyQL(ddb: DuckDBBackend): Unit =
    val query =
      if (set)
        tyqlDB.jumpOp
          .unrestrictedFix(flow =>
            flow.flatMap(f1 =>
              flow.filter(f2 => f1.b == f2.a).map(f2 =>
                (a = f1.a, b = f2.b).toRow)))
          .flatMap(f =>
            tyqlDB.readOp.flatMap(r =>
              tyqlDB.writeOp
                .filter(w => w.opN == f.a && w.varN == r.varN && f.b == r.opN)
                .map(w => (r = r.opN, w = w.opN).toRow)))
          .sort(_.r, Ord.ASC).sort(_.w, Ord.ASC)
      else
        tyqlDB.jumpOp
          .unrestrictedBagFix(flow =>
            flow.flatMap(f1 =>
              flow.filter(f2 => f1.b == f2.a).map(f2 =>
                (a = f1.a, b = f2.b).toRow)))
          .flatMap(f =>
            tyqlDB.readOp.flatMap(r =>
              tyqlDB.writeOp
                .filter(w => w.opN == f.a && w.varN == r.varN && f.b == r.opN)
                .map(w => (r = r.opN, w = w.opN).toRow)))
          .sort(_.r, Ord.ASC).sort(_.w, Ord.ASC)

    val queryStr = query.toQueryIR.toSQLString()
    resultTyql = ddb.runQuery(queryStr)

  def executeCollections(): Unit =
    val base = collectionsDB.jumpOp
    resultCollections = FixedPointQuery.fix(set)(base, Seq())(flow =>
      flow.flatMap(f1 =>
        flow
          .filter(f2 =>
            f1.b == f2.a)
          .map(f2 =>
            JumpCC(a = f1.a, b = f2.b))))
      .flatMap(f =>
        collectionsDB.readOp.flatMap(r =>
          collectionsDB.writeOp
            .filter(w => w.opN == f.a && w.varN == r.varN && f.b == r.opN)
            .map(w => ResultCC(r = r.opN, w = w.opN))))
      .sortBy(_.r).sortBy(_.w)


  def executeScalaSQL(ddb: DuckDBBackend): Unit =
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection
    val toTuple = (c: JumpSS[?]) => (c.a, c.b)

    val initBase = () =>
      dataflow_jumpOp.select.map(c => (c.a, c.b))

    val fixFn: ScalaSQLTable[JumpSS] => query.Select[(Expr[String], Expr[String]), (String, String)] = flow =>
      for {
        f1 <- flow.select
        f2 <- flow.join(f1.b === _.a)
      } yield (f1.a, f2.b)

    FixedPointQuery.scalaSQLSemiNaive(set)(
      ddb, dataflow_delta, dataflow_tmp, dataflow_derived
    )(toTuple)(initBase.asInstanceOf[() => query.Select[Any, Any]])(fixFn.asInstanceOf[ScalaSQLTable[JumpSS] => query.Select[Any, Any]])

// Multi-join not working
//    val result =
//      for {
//        f <- dataflow_derived.select
//        r <- dataflow_readOp.join(_.opN === f.b)
//        w <- dataflow_writeOp.join(_.opN === f.a).join(_.varN === r.varN)
//      } yield (r.opN, w.opN)

    val result = "SELECT readOp168.opN as r, writeOp169.opN as w " +
    s"FROM ${ScalaSQLTable.name(dataflow_derived)} as recref13, ${ScalaSQLTable.name(dataflow_readOp)} as readOp168, ${ScalaSQLTable.name(dataflow_writeOp)} as writeOp169 " +
    "WHERE writeOp169.opN = recref13.a AND writeOp169.varN = readOp168.varN AND recref13.b = readOp168.opN ORDER BY w ASC, r ASC;"
    backupResultScalaSql = ddb.runQuery(result)

  // Write results to csv for checking
  def writeTyQLResult(): Unit =
    val outfile = s"$outdir/tyql.csv"
    resultSetToCSV(resultTyql, outfile)

  def writeCollectionsResult(): Unit =
    val outfile = s"$outdir/collections.csv"
    collectionToCSV(resultCollections, outfile, Seq("r", "w"), fromCollRes)

  def writeScalaSQLResult(): Unit =
    val outfile = s"$outdir/scalasql.csv"
    if (backupResultScalaSql != null)
      resultSetToCSV(backupResultScalaSql, outfile)
    else
      collectionToCSV(resultScalaSQL, outfile, Seq("r", "w"), fromResultSS)

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
