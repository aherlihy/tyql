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
import tyql.*
import tyql.Query.{unrestrictedBagFix, unrestrictedFix, fix}
import tyql.Expr.{IntLit, StringLit, count}
import Helpers.*

@experimental
class TOPartyQuery extends QueryBenchmark {
  override def name = "party"
  override def set = false

  // TYQL data model
  type Organizer = (orgName: String)
  type Friend = (pName: String, fName: String)
  type PartyDB = (organizers: Organizer, friends: Friend, counts: (fName: String, nCount: Int))

  val tyqlDB = (
    organizers = Table[Organizer]("party_organizers"),
    friends = Table[Friend]("party_friends"),
    counts = Table[(fName: String, nCount: Int)]("party_counts")
  )

  // Collections data model + initialization
  case class OrganizerCC(orgName: String)
  case class FriendCC(pName: String, fName: String)
  case class CountsCC(fName: String, nCount: Int)
  case class ResultCC(person: String)
  def toCollRow1(row: Seq[String]): OrganizerCC = OrganizerCC(row(0).toString)
  def toCollRow2(row: Seq[String]): FriendCC = FriendCC(row(0).toString, row(1).toString)
  case class CollectionsDB(organizers: Seq[OrganizerCC], friends: Seq[FriendCC])
  def fromCollRes(r: ResultCC): Seq[String] = Seq(
    r.person.toString
  )
  var collectionsDB: CollectionsDB = null

  def initializeCollections(): Unit =
    val allCSV = getCSVFiles(datadir)
    val tables = allCSV.map(s => (s.getFileName.toString.replace(".csv", ""), s)).map((name, csv) =>
      val loaded = name match
        case "organizers" =>
          loadCSV(csv, toCollRow1)
        case "friends" =>
          loadCSV(csv, toCollRow2)
        case _ => ???
      (name, loaded)
    ).toMap
    collectionsDB = CollectionsDB(tables("organizers").asInstanceOf[Seq[OrganizerCC]], tables("friends").asInstanceOf[Seq[FriendCC]])

  //   ScalaSQL data model
  case class OrganizerSS[T[_]](orgName: T[String])
  case class FriendSS[T[_]](pName: T[String], fName: T[String])
  case class CountsSS[T[_]](fName: T[String], nCount: T[Int])
  case class ResultSS[T[_]](person: T[String])
  def fromSSRes(r: ResultSS[?]): Seq[String] = Seq(
    r.person.toString
  )

  object party_organizers extends ScalaSQLTable[OrganizerSS]
  object party_friends extends ScalaSQLTable[FriendSS]
  object party_counts extends ScalaSQLTable[CountsSS]
  object party_delta1 extends ScalaSQLTable[ResultSS]
  object party_derived1 extends ScalaSQLTable[ResultSS]
  object party_tmp1 extends ScalaSQLTable[ResultSS]
  object party_delta2 extends ScalaSQLTable[CountsSS]
  object party_derived2 extends ScalaSQLTable[CountsSS]
  object party_tmp2 extends ScalaSQLTable[CountsSS]

  // Result types for later printing
  var resultTyql: ResultSet = null
  var resultJDBC_RSQL: ResultSet = null
  var resultScalaSQL: Seq[ResultSS[?]] = null
  var resultCollections: Seq[ResultCC] = null
  var backupResultScalaSql: ResultSet = null

  // Execute queries
  def executeJDBC_RSQL(ddb: DuckDBBackend): Unit =
    val queryStr =
      "WITH RECURSIVE recursive1 AS ((SELECT party_organizers2.orgName as person FROM party_organizers as party_organizers2) UNION ALL ((SELECT ref1.fName as person FROM recursive2 as ref1 WHERE ref1.nCount > 2))),\nrecursive2 AS ((SELECT * FROM party_counts as party_counts6) UNION ALL ((SELECT party_friends8.pName as fName, COUNT(party_friends8.fName) as nCount FROM party_friends as party_friends8, recursive1 as ref4 WHERE ref4.person = party_friends8.fName GROUP BY party_friends8.pName)))\n SELECT * FROM recursive1 as recref0 ORDER BY person ASC"
    resultJDBC_RSQL = ddb.runQuery(queryStr)

  def executeUnrestrictedTyQL(ddb: DuckDBBackend): Unit =
    val baseAttend = tyqlDB.organizers.map(o => (person = o.orgName).toRow)
    val baseCntFriends = tyqlDB.counts

    val (finalAttend, finalCntFriends) = unrestrictedBagFix(baseAttend, baseCntFriends)((attend, cntfriends) =>
      val recurAttend = cntfriends
        .filter(cf => cf.nCount > 2)
        .map(cf => (person = cf.fName).toRow)

      val recurCntFriends = tyqlDB.friends
        .aggregate(friends =>
          attend
            .filter(att => att.person == friends.fName)
            .aggregate(att => (fName = friends.pName, nCount = count(friends.fName)).toGroupingRow)
        ).groupBySource(f => (name = f._1.pName).toRow)
      (recurAttend, recurCntFriends)
    )
    val query = finalAttend

    val queryStr = query.sort(_.person, Ord.ASC).toQueryIR.toSQLString()
    resultTyql = ddb.runQuery(queryStr)

  def executeCustomTyQL(ddb: DuckDBBackend): Unit =
    import RestrictedQuery.*
    val baseAttend = tyqlDB.organizers.map(o => (person = o.orgName).toRow)
    val baseCntFriends = tyqlDB.counts

    val partyOptions = (constructorFreedom = NonRestrictedConstructors(), monotonicity = NonMonotone(), category = BagResult(), linearity = Linear(), mutual = AllowMutual())
    val (finalAttend, finalCntFriends) = fix(partyOptions)(baseAttend, baseCntFriends)((attend, cntfriends) =>
      val recurAttend = cntfriends
        .filter(cf => cf.nCount > 2)
        .map(cf => (person = cf.fName).toRow)

      val recurCntFriends = tyqlDB.friends
        .aggregate(friends =>
          attend
            .filter(att => att.person == friends.fName)
            .aggregate(att => (fName = friends.pName, nCount = count(friends.fName)).toGroupingRow)
        ).groupBySource(f => (name = f._1.pName).toRow)
      (recurAttend, recurCntFriends)
    )
    val query = finalAttend

    val queryStr = query.sort(_.person, Ord.ASC).toQueryIR.toSQLString()
    resultTyql = ddb.runQuery(queryStr)

  def executeCollections(): Unit =
    var it = 0
    val baseAttend = collectionsDB.organizers.map(o => ResultCC(person = o.orgName))
    val baseCntFriends = Seq[CountsCC]()

    val (finalAttend, finalCntFriends) = FixedPointQuery.multiFix(set)((baseAttend, baseCntFriends), (Seq(), Seq()))((recur, acc) =>
      val (attend, cntfriends) = recur
      val (attendAcc, cntfriendsAcc) = if it == 0 then (baseAttend, baseCntFriends) else acc
      it+=1

      val recurAttend = cntfriendsAcc
        .filter(cf => cf.nCount > 2)
        .map(cf => ResultCC(person = cf.fName))

      val recurCntFriends = collectionsDB.friends
        .flatMap(friends =>
          if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
          attendAcc
            .filter(att =>
              if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
              att.person == friends.fName)
            .map(att =>
              if Thread.currentThread().isInterrupted then throw new Exception(s"$name timed out")
              FriendCC(friends.pName, friends.fName))
        )
        .groupBy(_.pName)
        .map((pName, pairs) => CountsCC(fName = pName, nCount = pairs.size))
        .toSeq

      (recurAttend, recurCntFriends)
    )
    resultCollections = finalAttend.sortBy(_.person)
    // println(s"\nIT,$name,collections,$it")


  def executeScalaSQL(ddb: DuckDBBackend): Unit =
    var it = 0
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection
    val toTuple1 = (c: ResultSS[?]) => c.person
    val toTuple2 = (c: CountsSS[?]) => (c.fName, c.nCount)

    val initBase = () =>
      val attend = party_organizers.select.map(o => (o.orgName))
      val cntFriends = party_counts.select.map(c => (c.fName, c.nCount))
      (attend, cntFriends)

    val fixFn: ((ScalaSQLTable[ResultSS], ScalaSQLTable[CountsSS])) => (String, String) = {
      recur =>
        val (attend, cntFriends) = recur
        val (attendAcc, cntFriendsAcc) = if it == 0 then (party_delta1, party_delta2) else (party_derived1, party_derived2)
        it+=1

        val recurAttend = s"SELECT f.fName as person FROM ${ScalaSQLTable.name(cntFriendsAcc)} as f WHERE f.nCount > 2"

        val recurFriends = s"SELECT f.pName as fName, COUNT(f.fName) as count FROM ${ScalaSQLTable.name(party_friends)} as f, ${ScalaSQLTable.name(attendAcc)} as a WHERE a.person = f.fName GROUP BY f.pName"

        (recurAttend, recurFriends)
    }

    FixedPointQuery.agg_scalaSQLSemiNaiveTWO(set)(
      ddb, (party_delta1, party_delta2), (party_tmp1, party_tmp2), (party_derived1, party_derived2)
    )(
      (toTuple1.asInstanceOf[ResultSS[?] => Tuple], toTuple2)
    )(
      initBase.asInstanceOf[() => (query.Select[Any, Any], query.Select[Any, Any])]
    )(fixFn)

    val result = party_derived1.select.sortBy(_.person)
    resultScalaSQL = db.run(result)
    // println(s"\nIT,$name,scalasql,$it")


  // Write results to csv for checking
  def writeJDBC_RSQLResult(): Unit =
    val outfile = s"$outdir/jdbc-rsql.csv"
    resultSetToCSV(resultJDBC_RSQL, outfile)

  def writeTyQLResult(): Unit =
    val outfile = s"$outdir/tyql.csv"
    resultSetToCSV(resultTyql, outfile)

  def writeCollectionsResult(): Unit =
    val outfile = s"$outdir/collections.csv"
    collectionToCSV(resultCollections, outfile, Seq("person"), fromCollRes)

  def writeScalaSQLResult(): Unit =
    val outfile = s"$outdir/scalasql.csv"
    if (backupResultScalaSql != null)
      resultSetToCSV(backupResultScalaSql, outfile)
    else
      collectionToCSV(resultScalaSQL, outfile, Seq("person"), fromSSRes)
}
