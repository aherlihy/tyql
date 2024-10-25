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
import tyql.Query.{unrestrictedFix, unrestrictedBagFix}
import tyql.Expr.{IntLit, StringLit, count}
import Helpers.*

@experimental
class TrustChainQuery extends QueryBenchmark {
  override def name = "trustchain"
  override def set = true

  // TYQL data model
  type Friends = (person1: String, person2: String)
  type TrustDB = (friends: Friends)

  val tyqlDB = (
    friends = Table[Friends]("trustchain_friends")
    )

  // Collections data model + initialization
  case class FriendsCC(person1: String, person2: String)
  case class ResultCC(name: String, count: Int)
  def toCollRow(row: Seq[String]): FriendsCC = FriendsCC(row(0).toString, row(1).toString)
  case class CollectionsDB(friends: Seq[FriendsCC])
  def fromCollRes(r: ResultCC): Seq[String] = Seq(
    r.name.toString,
    r.count.toString
  )
  var collectionsDB: CollectionsDB = null

  def initializeCollections(): Unit =
    val allCSV = getCSVFiles(datadir)
    val tables = allCSV.map(s => (s.getFileName.toString.replace(".csv", ""), s)).map((name, csv) =>
      val loaded = name match
        case "friends" =>
          loadCSV(csv, toCollRow)
        case _ => ???
      (name, loaded)
    ).toMap
    collectionsDB = CollectionsDB(tables("friends"))

  //   ScalaSQL data model
  case class FriendsSS[T[_]](person1: T[String], person2: T[String])
  case class ResultSS[T[_]](name: T[String], count: T[Int])
  def fromSSRes(r: ResultSS[?]): Seq[String] = Seq(
    r.name.toString,
    r.count.toString
  )

  object trustchain_friends extends ScalaSQLTable[FriendsSS]
  object trustchain_delta1 extends ScalaSQLTable[FriendsSS]
  object trustchain_derived1 extends ScalaSQLTable[FriendsSS]
  object trustchain_tmp1 extends ScalaSQLTable[FriendsSS]
  object trustchain_delta2 extends ScalaSQLTable[FriendsSS]
  object trustchain_derived2 extends ScalaSQLTable[FriendsSS]
  object trustchain_tmp2 extends ScalaSQLTable[FriendsSS]

  // Result types for later printing
  var resultTyql: ResultSet = null
  var resultScalaSQL: Seq[ResultSS[?]] = null
  var resultCollections: Seq[ResultCC] = null
  var backupResultScalaSql: ResultSet = null

  // Execute queries
  def executeTyQL(ddb: DuckDBBackend): Unit =
    val baseFriends = tyqlDB.friends

    val (trust, friends) = unrestrictedFix((baseFriends, baseFriends))((trust, friends) => {
      val mutualTrustResult = friends.flatMap(f =>
        trust
          .filter(mt => mt.person2 == f.person1)
          .map(mt => (person1 = mt.person1, person2 = f.person2).toRow)
      )

      val friendsResult = trust.map(mt =>
        (person1 = mt.person1, person2 = mt.person2).toRow
      )

      (mutualTrustResult, friendsResult)
    })

    val query = trust
      .aggregate(mt => (name = mt.person2, count = count(mt.person1)).toGroupingRow)
      .groupBySource(mt => (person = mt._1.person2).toRow).sort(mt => mt.name, Ord.ASC)

    val queryStr = query.toQueryIR.toSQLString()
    resultTyql = ddb.runQuery(queryStr)

  def executeCollections(): Unit =
    val baseFriends = collectionsDB.friends
//    println(s"Friends: $baseFriends")
//    println(s"Trust: $baseFriends")

//    var it = 0
    val (trust, friends) = FixedPointQuery.multiFix(set)((baseFriends, baseFriends), (Seq(), Seq()))((trust: Seq[FriendsCC], friends: Seq[FriendsCC]) => {
//      println(s"***iteration $it")
//      println(s"\ninput:\n\ttrust  : ${trust.map(f => f.person1 + "-" + f.person2).mkString("(", ",", ")")}\n\tfriends: ${friends.map(f => f.person1 + "-" + f.person2).mkString("(", ",", ")")}")

      it += 1
      val mutualTrustResult = friends.flatMap(f =>
        trust
          .filter(mt => mt.person2 == f.person1)
          .map(mt => FriendsCC(person1 = mt.person1, person2 = f.person2))
      )

      val friendsResult = trust.map(mt =>
        FriendsCC(person1 = mt.person1, person2 = mt.person2)
      )

//      println(s"output:\n\tRtrust : ${mutualTrustResult.map(f => f.person1 + "-" + f.person2).mkString("(", ",", ")")}\n\tRfriend: ${friendsResult.map(f => f.person1 + "-" + f.person2).mkString("(", ",", ")")}")
      (mutualTrustResult, friendsResult)
    })
//    println(s"FINAL: ${trust.map(f => f.person1 + "-" + f.person2).mkString("(", ",", ")")}, ${friends.map(f => f.person1 + "-" + f.person2).mkString("(", ",", ")")}")

    val query = trust
      .groupBy(_.person2)
      .map((person2, rest) => ResultCC(name = person2, count = rest.distinct.size))
      .toSeq

    resultCollections = query.sortBy(_.name)



  def executeScalaSQL(ddb: DuckDBBackend): Unit =
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection
    val toTuple = (c: FriendsSS[?]) => (c.person1, c.person2)

    val initBase = () =>
      val baseFriends = trustchain_friends.select.map(f => (f.person1, f.person2))
      val baseTrust = trustchain_friends.select.map(f => (f.person1, f.person2))
      (baseFriends, baseTrust)

//    var it = 0
    val fixFn: ((ScalaSQLTable[FriendsSS], ScalaSQLTable[FriendsSS])) => (query.Select[(Expr[String], Expr[String]), (String, String)], query.Select[(Expr[String], Expr[String]), (String, String)]) =
      recur =>
        val (trust, friends) = recur
//        println(s"***iteration $it")
//        it+=1
//        println(s"input:\n\ttrust : ${db.runRaw[(String, String)](s"SELECT * FROM ${ScalaSQLTable.name(trust)}").map(f => f._1 + "-" + f._2).mkString("(", ",", ")")}\n\tfriends: ${db.runRaw[(String, String)](s"SELECT * FROM ${ScalaSQLTable.name(friends)}").map(f => f._1 + "-" + f._2).mkString("(", ",", ")")}")
        val mut = for {
          f <- friends.select
          mt <- trust.crossJoin()
          if mt.person2 === f.person1
        } yield (mt.person1, f.person2)

        val fr = trust.select.map(t => (t.person1, t.person2))
//        println(s"output:\n\tmutual: ${db.run(mut).map(f => f._1 + "-" + f._2).mkString("(", ",", ")")}\n\tfriends: ${db.run(fr).map(f => f._1 + "-" + f._2).mkString("(", ",", ")")}")
        (mut, fr)

    // Fix point only on target result?
    FixedPointQuery.scalaSQLSemiNaive2(set)(
      db, (trustchain_delta1, trustchain_delta2), (trustchain_derived1, trustchain_derived2), (trustchain_tmp1, trustchain_tmp2)
    )(
      (toTuple, toTuple)
    )(
      initBase.asInstanceOf[() => (query.Select[Any, Any], query.Select[Any, Any])]
    )(fixFn.asInstanceOf[((ScalaSQLTable[FriendsSS], ScalaSQLTable[FriendsSS])) => (query.Select[Any, Any], query.Select[Any, Any])])

    backupResultScalaSql = ddb.runQuery(s"SELECT r.person2, COUNT(r.person1) as count FROM ${ScalaSQLTable.name(trustchain_derived2)} as r GROUP BY r.person2 ORDER BY r.person2")

  // Write results to csv for checking
  def writeTyQLResult(): Unit =
    val outfile = s"$outdir/tyql.csv"
    resultSetToCSV(resultTyql, outfile)

  def writeCollectionsResult(): Unit =
    val outfile = s"$outdir/collections.csv"
    collectionToCSV(resultCollections, outfile, Seq("name", "count"), fromCollRes)

  def writeScalaSQLResult(): Unit =
    val outfile = s"$outdir/scalasql.csv"
    if (backupResultScalaSql != null)
      resultSetToCSV(backupResultScalaSql, outfile)
    else
      collectionToCSV(resultScalaSQL, outfile, Seq("name", "count"), fromSSRes)

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
