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
import tyql.Query.{unrestrictedBagFix, unrestrictedFix}
import tyql.Expr.{IntLit, StringLit, min}
import Helpers.*

@experimental
class CBAQuery extends QueryBenchmark {
  override def name = "cba"
  override def set = false

  // TYQL data model
  type Term = (x: Int, y: String, z: Int)
  type Lits = (x: Int, y: String)
  type Vars = (x: Int, y: String)
  type Abs = (x: Int, y: Int, z: Int)
  type App = (x: Int, y: Int, z: Int)
  type BaseData = (x: Int, y: String)
  type BaseCtrl = (x: Int, y: Int)

  type CBADB = (term: Term, lits: Lits, vars: Vars, abs: Abs, app: App, baseData: BaseData, baseCtrl: BaseCtrl)

  val tyqlDB = (
    term = Table[Term]("cba_term"),
    lits = Table[Lits]("cba_lits"),
    vars = Table[Vars]("cba_vars"),
    abs = Table[Abs]("cba_abs"),
    app = Table[App]("cba_app"),
    baseData = Table[BaseData]("cba_baseData"),
    baseCtrl = Table[BaseCtrl]("cba_baseCtrl")
  )

  // Collections data model + initialization
  case class TermCC(x: Int, y: String, z: Int)
  case class LitsCC(x: Int, y: String)
  case class VarsCC(x: Int, y: String)
  case class AbsCC(x: Int, y: Int, z: Int)
  case class AppCC(x: Int, y: Int, z: Int)
  case class DataCC(x: Int, y: String)
  case class CtrlCC(x: Int, y: Int)

  def toCollRowTerm(row: Seq[String]): TermCC = TermCC(row(0).toInt, row(1).toString, row(2).toInt)
  def toCollRowLits(row: Seq[String]): LitsCC = LitsCC(row(0).toInt, row(1).toString)
  def toCollRowVars(row: Seq[String]): VarsCC = VarsCC(row(0).toInt, row(1).toString)
  def toCollRowAbs(row: Seq[String]): AbsCC = AbsCC(row(0).toInt, row(1).toInt, row(2).toInt)
  def toCollRowApp(row: Seq[String]): AppCC = AppCC(row(0).toInt, row(1).toInt, row(2).toInt)

  case class CollectionsDB(term: Seq[TermCC], lits: Seq[LitsCC], vars: Seq[VarsCC], abs: Seq[AbsCC], app: Seq[AppCC])

  def fromCollRes(r: DataCC): Seq[String] = Seq(
    r.x.toString,
    r.y.toString
  )
  var collectionsDB: CollectionsDB = null

  def initializeCollections(): Unit =
    val allCSV = getCSVFiles(datadir)
    val tables = allCSV.map(s => (s.getFileName.toString.replace(".csv", ""), s)).map((name, csv) =>
      val loaded = name match
        case "abs" =>
          loadCSV(csv, toCollRowAbs)
        case "app" =>
          loadCSV(csv, toCollRowApp)
        case "lits" =>
          loadCSV(csv, toCollRowLits)
        case "term" =>
          loadCSV(csv, toCollRowTerm)
        case "vars" =>
          loadCSV(csv, toCollRowVars)
        case _ => ???
      (name, loaded)
    ).toMap
    collectionsDB = CollectionsDB(
      tables("term").asInstanceOf[Seq[TermCC]],
      tables("lits").asInstanceOf[Seq[LitsCC]],
      tables("vars").asInstanceOf[Seq[VarsCC]],
      tables("abs").asInstanceOf[Seq[AbsCC]],
      tables("app").asInstanceOf[Seq[AppCC]])

  //   ScalaSQL data model
  case class TermSS[T[_]](x: T[Int], y: T[String], z: T[Int])
  case class LitsSS[T[_]](x: T[Int], y: T[String])
  case class VarsSS[T[_]](x: T[Int], y: T[String])
  case class AbsSS[T[_]](x: T[Int], y: T[Int], z: T[Int])
  case class AppSS[T[_]](x: T[Int], y: T[Int], z: T[Int])
  case class DataSS[T[_]](x: T[Int], y: T[String])
  case class CtrlSS[T[_]](x: T[Int], y: T[Int])

  def fromSSRes(r: DataSS[?]): Seq[String] = Seq(
    r.x.toString,
    r.y.toString
  )

  object cba_term extends ScalaSQLTable[TermSS]
  object cba_lits extends ScalaSQLTable[LitsSS]
  object cba_vars extends ScalaSQLTable[VarsSS]
  object cba_abs extends ScalaSQLTable[AbsSS]
  object cba_app extends ScalaSQLTable[AppSS]
  object cba_baseData extends ScalaSQLTable[DataSS]
  object cba_baseCtrl extends ScalaSQLTable[CtrlSS]

  object cba_delta1 extends ScalaSQLTable[DataSS]
  object cba_derived1 extends ScalaSQLTable[DataSS]
  object cba_tmp1 extends ScalaSQLTable[DataSS]
  object cba_delta2 extends ScalaSQLTable[DataSS]
  object cba_derived2 extends ScalaSQLTable[DataSS]
  object cba_tmp2 extends ScalaSQLTable[DataSS]
  object cba_delta3 extends ScalaSQLTable[CtrlSS]
  object cba_derived3 extends ScalaSQLTable[CtrlSS]
  object cba_tmp3 extends ScalaSQLTable[CtrlSS]
  object cba_delta4 extends ScalaSQLTable[CtrlSS]
  object cba_derived4 extends ScalaSQLTable[CtrlSS]
  object cba_tmp4 extends ScalaSQLTable[CtrlSS]

  // Result types for later printing
  var resultTyql: ResultSet = null
  var resultScalaSQL: Seq[DataSS[?]] = null
  var resultCollections: Seq[DataCC] = null
  var backupResultScalaSql: ResultSet = null

  // Execute queries
  def executeTyQL(ddb: DuckDBBackend): Unit =
    val dataTermBase = tyqlDB.term.flatMap(t =>
      tyqlDB.lits
        .filter(l => l.x == t.z && t.y == StringLit("Lit"))
        .map(l => (x = t.x, y = l.y).toRow))

    val dataVarBase = tyqlDB.baseData

    val ctrlTermBase = tyqlDB.term.filter(t => t.y == StringLit("Abs")).map(t => (x = t.x, y = t.z).toRow)

    val ctrlVarBase = tyqlDB.baseCtrl

    val fn = if set then unrestrictedFix((dataTermBase, dataVarBase, ctrlTermBase, ctrlVarBase)) else unrestrictedBagFix((dataTermBase, dataVarBase, ctrlTermBase, ctrlVarBase))

    val (dataTerm, dataVar, ctrlTerm, ctrlVar) = fn(
      (dataTerm, dataVar, ctrlTerm, ctrlVar) => {
        val dt1 =
          for
            t <- tyqlDB.term
            dv <- dataVar
            if t.y == "Var" && t.z == dv.x
          yield (x = t.x, y = dv.y).toRow

        val dt2 =
          for
            t <- tyqlDB.term
            dt <- dataTerm
            ct <- ctrlTerm
            abs <- tyqlDB.abs
            app <- tyqlDB.app
            if t.y == "App" && t.z == app.x && dt.x == abs.z && ct.x == app.y && ct.y == abs.x
          yield (x = t.x, y = dt.y).toRow

        val dv =
          for
            ct <- ctrlTerm
            dt <- dataTerm
            abs <- tyqlDB.abs
            app <- tyqlDB.app
            if ct.x == app.y && ct.y == abs.x && dt.x == app.z
          yield (x = abs.y, y = dt.y).toRow

        val ct1 =
          for
            t <- tyqlDB.term
            cv <- ctrlVar
            if t.y == "Var" && t.z == cv.x
          yield (x = t.x, y = cv.y).toRow
        val ct2 =
          for
            t <- tyqlDB.term
            ct1 <- ctrlTerm
            ct2 <- ctrlTerm
            abs <- tyqlDB.abs
            app <- tyqlDB.app
            if t.y == "App" && t.z == app.x && ct1.x == abs.z && ct2.x == app.y && ct2.y == abs.x
          yield (x = t.x, y = ct1.y).toRow

        val cv =
          for
            ct1 <- ctrlTerm
            ct2 <- ctrlTerm
            abs <- tyqlDB.abs
            app <- tyqlDB.app
            if ct1.x == app.y && ct1.y == abs.x && ct2.x == app.z
          yield (x = abs.y, y = ct2.y).toRow

        val dt = if set then dt1.union(dt2) else dt1.unionAll(dt2)
        val ct = if set then ct1.union(ct2) else ct1.unionAll(ct2)

        (dt, dv, ct, cv)
      })

//    dataTerm.distinct.size
    val queryStr = dataTerm.sort(_.y, Ord.ASC).sort(_.x, Ord.ASC).toQueryIR.toSQLString().replace("\"", "'")
    resultTyql = ddb.runQuery(queryStr)

  def executeCollections(): Unit =
    val dataTermBase = collectionsDB.term.flatMap(t =>
      collectionsDB.lits
        .filter(l => l.x == t.z && t.y == "Lit")
        .map(l => DataCC(x = t.x, y = l.y)))

    val dataVarBase = Seq[DataCC]()

    val ctrlTermBase = collectionsDB.term.filter(t => t.y == "Abs").map(t => CtrlCC(x = t.x, y = t.z))

    val ctrlVarBase = Seq[CtrlCC]()

    var it = 0
    val (dataTerm, dataVar, ctrlTerm, ctrlVar) = FixedPointQuery.multiFix(set)((dataTermBase, dataVarBase, ctrlTermBase, ctrlVarBase), (Seq[DataCC](),Seq[DataCC](), Seq[CtrlCC](), Seq[CtrlCC]()))(
      (recur, acc) => {
        val (dataTerm, dataVar, ctrlTerm, ctrlVar) = recur
        val (dataTermAcc, dataVarAcc, ctrlTermAcc, ctrlVarAcc) = if it == 0 then (dataTermBase, dataVarBase, ctrlTermBase, ctrlVarBase) else acc
        it += 1

        val dt1 =
          for
            t <- collectionsDB.term
            dv <- dataVarAcc
            if t.y == "Var" && t.z == dv.x
          yield DataCC(x = t.x, y = dv.y)

        val dt2 =
          for
            t <- collectionsDB.term
            dt <- dataTerm
            ct <- ctrlTermAcc
            abs <- collectionsDB.abs
            app <- collectionsDB.app
            if t.y == "App" && t.z == app.x && dt.x == abs.z && ct.x == app.y && ct.y == abs.x
          yield DataCC(x = t.x, y = dt.y)

        val dv =
          for
            ct <- ctrlTermAcc
            dt <- dataTermAcc
            abs <- collectionsDB.abs
            app <- collectionsDB.app
            if ct.x == app.y && ct.y == abs.x && dt.x == app.z
          yield DataCC(x = abs.y, y = dt.y)

        val ct1 =
          for
            t <- collectionsDB.term
            cv <- ctrlVarAcc
            if t.y == "Var" && t.z == cv.x
          yield CtrlCC(x = t.x, y = cv.y)
        val ct2 =
          for
            t <- collectionsDB.term
            ct1 <- ctrlTerm
            ct2 <- ctrlTerm
            abs <- collectionsDB.abs
            app <- collectionsDB.app
            if t.y == "App" && t.z == app.x && ct1.x == abs.z && ct2.x == app.y && ct2.y == abs.x
          yield CtrlCC(x = t.x, y = ct1.y)

        val cv =
          for
            ct1 <- ctrlTermAcc
            ct2 <- ctrlTermAcc
            abs <- collectionsDB.abs
            app <- collectionsDB.app
            if ct1.x == app.y && ct1.y == abs.x && ct2.x == app.z
          yield CtrlCC(x = abs.y, y = ct2.y)

        (dt1 ++ dt2, dv, ct1 ++ ct2, cv)
      })

      resultCollections = dataTerm.sortBy(_.y).sortBy(_.x)


  def executeScalaSQL(ddb: DuckDBBackend): Unit =
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection
    val toTuple1 = (c: DataSS[?]) => (c.x, c.y)
    val toTuple2 = (c: CtrlSS[?]) => (c.x, c.y)

    val initBase = () => {
      val dataTermBase = for {
        t <- cba_term.select
        l <- cba_lits.crossJoin()
        if l.x === t.z && t.y === Expr("Lit")
      } yield (t.x, l.y)

      val dataVarBase = cba_baseData.select.map(f => (f.x, f.y))

      val ctrlTermBase = cba_term.select.filter(t => t.y === Expr("Abs")).map(t => (t.x, t.z))

      val ctrlVarBase = cba_baseCtrl.select.map(f => (f.x, f.y))
      (dataTermBase, dataVarBase, ctrlTermBase, ctrlVarBase)
    }

    var it = 0
    val fixFn: ((ScalaSQLTable[DataSS], ScalaSQLTable[DataSS], ScalaSQLTable[CtrlSS], ScalaSQLTable[CtrlSS])) => (query.Select[(Expr[Int], Expr[String]), (Int, String)], query.Select[(Expr[Int], Expr[String]), (Int, String)], query.Select[(Expr[Int], Expr[Int]), (Int, Int)], query.Select[(Expr[Int], Expr[Int]), (Int, Int)]) =
      recur => {
        val (dataTerm, dataVar, ctrlTerm, ctrlVar) = recur
        val (dataTermAcc, dataVarAcc, ctrlTermAcc, ctrlVarAcc) = if it == 0 then (cba_delta1, cba_delta2, cba_delta3, cba_delta4) else (cba_derived1, cba_derived2, cba_derived3, cba_derived4)
        it += 1

        //        println(s"***iteration $it")
        //        println(s"BASES:\n\teven : ${db.runRaw[(String, String)](s"SELECT * FROM ${ScalaSQLTable.name(even)}").map(f => f._1 + "-" + f._2).mkString("(", ",", ")")}\n\todd: ${db.runRaw[(String, String)](s"SELECT * FROM ${ScalaSQLTable.name(odd)}").map(f => f._1 + "-" + f._2).mkString("(", ",", ")")}")
        //        println(s"DERIV:\n\teven : ${db.runRaw[(String, String)](s"SELECT * FROM ${ScalaSQLTable.name(derived_even)}").map(f => f._1 + "-" + f._2).mkString("(", ",", ")")}\n\todd: ${db.runRaw[(String, String)](s"SELECT * FROM ${ScalaSQLTable.name(derived_odd)}").map(f => f._1 + "-" + f._2).mkString("(", ",", ")")}")
        val dataTerm1 =
          for
            t <- cba_term.select
            dv <- dataVarAcc.crossJoin()
            if t.y === "Var" && t.z === dv.x
          yield (t.x, dv.y)

        val dataTerm2 =
          for
            t <- cba_term.select
            dt <- dataTerm.crossJoin()
            ct <- ctrlTerm.crossJoin()
            abs <- cba_abs.crossJoin()
            app <- cba_app.crossJoin()
            if t.y === "App" && t.z === app.x && dt.x === abs.z && ct.x === app.y && ct.y === abs.x
          yield (t.x, dt.y)

        val dataVarResult =
          for
            ct <- ctrlTermAcc.select
            dt <- dataTermAcc.crossJoin()
            abs <- cba_abs.crossJoin()
            app <- cba_app.crossJoin()
            if ct.x === app.y && ct.y === abs.x && dt.x === app.z
          yield (abs.y, dt.y)

        val controlTerm1 =
          for
            t <- cba_term.select
            cv <- ctrlVarAcc.crossJoin()
            if t.y === "Var" && t.z === cv.x
          yield (t.x, cv.y)
        val controlTerm2 =
          for
            t <- cba_term.select
            ct1 <- ctrlTerm.crossJoin()
            ct2 <- ctrlTerm.crossJoin()
            abs <- cba_abs.crossJoin()
            app <- cba_app.crossJoin()
            if t.y === "App" && t.z === app.x && ct1.x === abs.z && ct2.x === app.y && ct2.y === abs.x
          yield (t.x, ct1.y)

        val controlVarResult =
          for
            ct1 <- ctrlTermAcc.select
            ct2 <- ctrlTermAcc.crossJoin()
            abs <- cba_abs.crossJoin()
            app <- cba_app.crossJoin()
            if ct1.x === app.y && ct1.y === abs.x && ct2.x === app.z
          yield (abs.y, ct2.y)

        //        println(s"output:\n\teven: ${db.run(evenResult).map(f => f._1 + "-" + f._2).mkString("(", ",", ")")}\n\toddResult: ${db.run(oddResult).map(f => f._1 + "-" + f._2).mkString("(", ",", ")")}")
        (dataTerm1.unionAll(dataTerm2), dataVarResult, controlTerm1.unionAll(controlTerm2), controlVarResult)

      }

    FixedPointQuery.scalaSQLSemiNaiveFOUR(set)(
      db, (cba_delta1, cba_delta2, cba_delta3, cba_delta4), (cba_tmp1, cba_tmp2, cba_tmp3, cba_tmp4), (cba_derived1, cba_derived2, cba_derived3, cba_derived4)
    )(
      (toTuple1, toTuple1, toTuple2, toTuple2)
    )(
      initBase.asInstanceOf[() => (query.Select[Any, Any], query.Select[Any, Any], query.Select[Any, Any], query.Select[Any, Any])]
    )(fixFn.asInstanceOf[((ScalaSQLTable[DataSS], ScalaSQLTable[DataSS], ScalaSQLTable[CtrlSS], ScalaSQLTable[CtrlSS])) => (query.Select[Any, Any], query.Select[Any, Any], query.Select[Any, Any], query.Select[Any, Any])])

    val result = cba_derived1.select.sortBy(_.y).sortBy(_.x)
    resultScalaSQL = db.run(result)

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
