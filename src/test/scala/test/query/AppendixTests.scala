package test.query.appendixTests
import test.{SQLStringQueryTest, TestDatabase, SQLStringAggregationTest}
import tyql.*
import Query.{customFix, fix, unrestrictedBagFix, unrestrictedFix}
import Expr.{IntLit, StringLit, count, max, min, sum}

import language.experimental.namedTuples
import NamedTuple.*

type EmptyDB = (nothing: EmptyTuple)
given PlaceholderDB: TestDatabase[EmptyDB] with
  override def tables = (
    nothing = Table[EmptyTuple]("nothing")
  )

class EvenOddAppendixTest extends SQLStringQueryTest[EmptyDB, (value: Int, typ: String)] {
  def testDescription: String = "EvenOdd"

  def appendixSQLString: String = """
CREATE TABLE Numbers ( id INTEGER, value INTEGER );
WITH RECURSIVE
    evenR AS
        ((SELECT Numbers$2.value as value, 'even' as typ
          FROM Numbers as Numbers$2
          WHERE Numbers$2.value = 0)
            UNION
        ((SELECT Numbers$4.value as value, 'even' as typ
          FROM Numbers as Numbers$4, oddR as ref$5
          WHERE Numbers$4.value = ref$5.value + 1))),
    oddR AS
        ((SELECT Numbers$8.value as value, 'odd' as typ
          FROM Numbers as Numbers$8
          WHERE Numbers$8.value = 1)
            UNION
        ((SELECT Numbers$10.value as value, 'odd' as typ
          FROM Numbers as Numbers$10, evenR as ref$8
          WHERE Numbers$10.value = ref$8.value + 1)))
SELECT * FROM oddR as recref$1
  """

  def query() =
    val options = (
      constructorFreedom = NonRestrictedConstructors(),
      monotonicity = Monotone(),
      category = SetResult(),
      linearity = NonLinear() // TODO
    )
    val numbers = Table[(id: Int, value: Int)]("Numbers")
    val evenBase = numbers
      .filter(n => n.value == IntLit(0))
      .map(n => (value = n.value, typ = StringLit("even")).toRow)
    val oddBase = numbers
      .filter(n => n.value == IntLit(1))
      .map(n => (value = n.value, typ = StringLit("odd")).toRow)
    val result =
      customFix(options)((evenBase, oddBase))((even, odd) =>
        val evenResult = numbers.flatMap(num =>
          odd
            .filter(o => num.value == o.value + IntLit(1))
            .map(o => (value = num.value, typ = StringLit("even")).toRow)
        ).distinct
        val oddResult = numbers.flatMap(num =>
          even
            .filter(e => num.value == e.value + IntLit(1))
            .map(e => (value = num.value, typ = StringLit("odd")).toRow)
        ).distinct
        (evenResult, oddResult))
    result._2
  def expectedQueryPattern: String =
    appendixSQLString
      .replaceAll("\'", "\"")
      .replaceAll("(?m)^CREATE TABLE.*", "")
      .replaceAll("evenR", "recursive\\$A")
      .replaceAll("oddR", "recursive\\$B")
}



class CompanyControlAppendixTest extends SQLStringQueryTest[EmptyDB, (com1: String, com2: String)] {
  def testDescription: String = "CompanyControl"
  def appendixSQLString: String = """
CREATE TABLE Shares ( byC TEXT, of TEXT, percent INT );
CREATE TABLE Control ( com1 TEXT, com2 TEXT );
WITH RECURSIVE
    csharesR AS
        ((SELECT *
          FROM Shares as Shares$2)
            UNION
        ((SELECT ref$22.com1 as byC, ref$23.of as of, SUM(ref$23.percent) as percent
          FROM controlR as ref$22, csharesR as ref$23
          WHERE ref$23.byC = ref$22.com2
          GROUP BY ref$22.com1, ref$23.of))),
    controlR AS
        ((SELECT *
          FROM Control as Control$4)
            UNION
        ((SELECT ref$27.byC as com1, ref$27.of as com2
          FROM csharesR as ref$27
          WHERE ref$27.percent > 50)))
SELECT * FROM controlR as recref$4
  """

  def query() =
    val options = (
      constructorFreedom = NonRestrictedConstructors(),
      monotonicity = NonMonotone(),
      category = SetResult(),
      linearity = Linear()
    )
    val shares = Table[(byC: String, of: String, percent: Int)]("Shares")
    val control = Table[(com1: String, com2: String)]("Control")
    val result =
      unrestrictedFix(shares, control)((csharesR, controlR) => // TODO
//      customFix(options)(shares, control)((csharesR, controlR) =>
        val csharesRecur = controlR
          .aggregate(con =>
            csharesR
              .filter(cs => cs.byC == con.com2)
              .aggregate(cs => (byC = con.com1, of = cs.of,
                percent = sum(cs.percent)).toGroupingRow))
          .groupBySource(
            (con, csh) => (byC = con.com1, of = csh.of).toRow)
          .distinct
        val controlRecur = csharesR
          .filter(s => s.percent > 50)
          .map(s => (com1 = s.byC, com2 = s.of).toRow)
          .distinct
        (csharesRecur, controlRecur))
    result._2
  def expectedQueryPattern: String =
    appendixSQLString
      .replaceAll("\'", "\"")
      .replaceAll("(?m)^CREATE TABLE.*", "")
      .replaceAll("csharesR", "recursive\\$A")
      .replaceAll("controlR", "recursive\\$B")
}

class TrustChainAppendixTest extends SQLStringQueryTest[EmptyDB, (person: String, count: Int)] {
  def testDescription: String = "COT"

  def appendixSQLString: String = """
CREATE TABLE Friends ( person1 TEXT, person2 TEXT );
WITH RECURSIVE
  trustR AS
      ((SELECT * FROM Friends as Friends$2)
        UNION ALL
      ((SELECT ref$127.person1 as person1, ref$126.person2 as person2
        FROM friendsR as ref$126, trustR as ref$127
        WHERE ref$127.person2 = ref$126.person1))),
  friendsR AS
      ((SELECT * FROM Friends as Friends$3)
        UNION ALL
      ((SELECT ref$129.person1 as person1, ref$129.person2 as person2
        FROM trustR as ref$129)))
SELECT recref$19.person2 as person, COUNT(recref$19.person1) as count
FROM trustR as recref$19
GROUP BY recref$19.person2
  """

  def query() = {
    val options = (
      constructorFreedom = RestrictedConstructors(),
      monotonicity = Monotone(),
      category = BagResult(),
      linearity = NonLinear() // TODO
    )
    val friends = Table[(person1: String, person2: String)]("Friends")

    val result =
        customFix(options)(friends, friends)((trustR, friendsR) => {
          val mutualTrustResult = friendsR.flatMap(f =>
          trustR
            .filter(mt => mt.person2 == f.person1)
            .map(mt => (person1 = mt.person1, person2 = f.person2).toRow)
          )

          val friendsResult = trustR.map(mt =>
            (person1 = mt.person1, person2 = mt.person2).toRow
          )

          (mutualTrustResult, friendsResult)
      })

    result._1
      .aggregate(mt => (person = mt.person2, count = count(mt.person1)).toGroupingRow)
      .groupBySource(mt => (person = mt._1.person2).toRow)
  }

  def expectedQueryPattern: String =
    appendixSQLString
      .replaceAll("\'", "\"")
      .replaceAll("(?m)^CREATE TABLE.*", "")
      .replaceAll("trustR", "recursive\\$A")
      .replaceAll("friendsR", "recursive\\$B")

}

class CSPAAppendixTest extends SQLStringQueryTest[EmptyDB, (p1: Int, p2: Int)] {
  def testDescription: String = "CSPA"
  def query() =
    val options = (
      constructorFreedom = RestrictedConstructors(),
      monotonicity = Monotone(),
      category = SetResult(),
      linearity = NonLinear()
    )
    val assign = Table[(p1: Int, p2: Int)]("Assign")
    val dereference = Table[(p1: Int, p2: Int)]("Dereference")

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

    val (valueFlowFinal, valueAliasFinal, memoryAliasFinal) =
      customFix(options)(valueFlowBase, Table[(p1: Int, p2: Int)]("empty"), memoryAliasBase)(
        (valueFlowR, valueAliasR, memoryAliasR) =>
          val vfDef1 =
            for
              a <- assign
              m <- memoryAliasR
              if a.p2 == m.p1
            yield (p1 = a.p1, p2 = m.p2).toRow
          val vfDef2 =
            for
              vf1 <- valueFlowR
              vf2 <- valueFlowR
              if vf1.p2 == vf2.p1
            yield (p1 = vf1.p1, p2 = vf2.p2).toRow
          val VF = vfDef1.union(vfDef2)

          val MA =
            for
              d1 <- dereference
              va <- valueAliasR
              d2 <- dereference
              if d1.p1 == va.p1 && va.p2 == d2.p1
            yield (p1 = d1.p2, p2 = d2.p2).toRow

          val vaDef1 =
            for
              vf1 <- valueFlowR
              vf2 <- valueFlowR
              if vf1.p1 == vf2.p1
            yield (p1 = vf1.p2, p2 = vf2.p2).toRow
          val vaDef2 =
            for
              vf1 <- valueFlowR
              m <- memoryAliasR
              vf2 <- valueFlowR
              if vf1.p1 == m.p1 && vf2.p1 == m.p2
            yield (p1 = vf1.p2, p2 = vf2.p2).toRow
          val VA = vaDef1.union(vaDef2)

          (VF, MA.distinct, VA)
      )
    valueFlowFinal
  def appendixSQLString: String =
    """
CREATE TABLE Dereference ( p1 INT, p2 INT );
CREATE TABLE empty ( p1 INT, p2 INT );
CREATE TABLE Assign ( p1 INT, p2 INT );
WITH RECURSIVE
  valueFlowR AS
  ((SELECT *
    FROM Assign as Assign$D)
    UNION
    ((SELECT Assign$E.p1 as p1, Assign$E.p1 as p2
      FROM Assign as Assign$E)
    UNION
    (SELECT Assign$F.p2 as p1, Assign$F.p2 as p2
    FROM Assign as Assign$F)
UNION
  (SELECT Assign$G.p1 as p1, ref$J.p2 as p2
FROM Assign as Assign$G, memoryAliasR as ref$J
WHERE Assign$G.p2 = ref$J.p1)
UNION
  (SELECT ref$K.p1 as p1, ref$L.p2 as p2
FROM valueFlowR as ref$K, valueFlowR as ref$L
WHERE ref$K.p2 = ref$L.p1))),
valueAliasR AS
  ((SELECT *
    FROM empty as empty$M)
    UNION
    ((SELECT Dereference$N.p2 as p1, Dereference$O.p2 as p2
      FROM Dereference as Dereference$N, valueAliasR as ref$P, Dereference as Dereference$O
      WHERE Dereference$N.p1 = ref$P.p1 AND ref$P.p2 = Dereference$O.p1))),
memoryAliasR AS
  ((SELECT Assign$H.p2 as p1, Assign$H.p2 as p2
    FROM Assign as Assign$H)
UNION
  ((SELECT Assign$I.p1 as p1, Assign$I.p1 as p2
    FROM Assign as Assign$I)
UNION
  (SELECT ref$Q.p2 as p1, ref$R.p2 as p2
FROM valueFlowR as ref$Q, valueFlowR as ref$R
WHERE ref$Q.p1 = ref$R.p1)
UNION
  (SELECT ref$S.p2 as p1, ref$T.p2 as p2
FROM valueFlowR as ref$S, memoryAliasR as ref$U, valueFlowR as ref$T
WHERE ref$S.p1 = ref$U.p1 AND ref$T.p1 = ref$U.p2)))
SELECT * FROM valueFlowR as recref$V
    """

  def expectedQueryPattern: String =
    appendixSQLString
      .replaceAll("\'", "\"")
      .replaceAll("(?m)^CREATE TABLE.*", "")
      .replaceAll("valueFlowR", "recursive\\$A")
      .replaceAll("valueAliasR", "recursive\\$B")
      .replaceAll("memoryAliasR", "recursive\\$C")
}

class PTCAppendixTest extends SQLStringAggregationTest[EmptyDB, Int] {
  def testDescription: String = "PTC"
  def query() =
    val options = (
      constructorFreedom = RestrictedConstructors(),
      monotonicity = Monotone(),
      category = SetResult(),
      linearity = NonLinear()
    )
    val newT = Table[(x: String, y: String)]("New")
    val assign = Table[(x: String, y: String)]("Assign")
    val store = Table[(x: String, y: String, h: String)]("Store")
    val loadT = Table[(x: String, y: String, h: String)]("LoadT")

    val baseVPT = newT.map(a => (x = a.x, y = a.y).toRow)
    val baseHPT = Table[(x: String, y: String, h: String)]("BaseHPT")

    val pt =
      customFix(options)((baseVPT, baseHPT))((varPointsTo, heapPointsTo) =>
        val vpt = assign.flatMap(a =>
          varPointsTo.filter(p => a.y == p.x).map(p =>
            (x = a.x, y = p.y).toRow
          )
        ).union(
          loadT.flatMap(l =>
            heapPointsTo.flatMap(hpt =>
              varPointsTo
                .filter(vpt => l.y == vpt.x && l.h == hpt.y && vpt.y == hpt.x)
                .map(pt2 =>
                  (x = l.x, y = hpt.h).toRow))))
        val hpt = store.flatMap(s =>
          varPointsTo.flatMap(vpt1 =>
            varPointsTo
              .filter(vpt2 => s.x == vpt1.x && s.h == vpt2.x)
              .map(vpt2 =>
                (x = vpt1.y, y = s.y, h = vpt2.y).toRow
              ))).distinct
        (vpt, hpt)
      )
    pt._1.filter(vpt => vpt.x == "r").size

  def appendixSQLString: String =
    """
CREATE TABLE New ( x TEXT, y TEXT );
CREATE TABLE Assign ( x TEXT, y TEXT );
CREATE TABLE LoadT ( x TEXT, y TEXT, h TEXT );
CREATE TABLE Store ( x TEXT, y TEXT, h TEXT );
CREATE TABLE BaseHPT ( x TEXT, y TEXT, h TEXT );
WITH RECURSIVE
    varPointsTo AS
        ((SELECT New$2.x as x, New$2.y as y
          FROM New as New$2)
            UNION
        ((SELECT Assign$4.x as x, ref$2.y as y
          FROM Assign as Assign$4, varPointsTo as ref$2
          WHERE Assign$4.y = ref$2.x)
            UNION
        (SELECT LoadT$7.x as x, ref$5.h as y
         FROM LoadT as LoadT$7, heapPointsTo as ref$5, varPointsTo as ref$6
         WHERE LoadT$7.y = ref$6.x AND LoadT$7.h = ref$5.y AND ref$6.y = ref$5.x))),
    heapPointsTo AS
        ((SELECT *
          FROM BaseHPT as BaseHPT$13)
            UNION
        ((SELECT ref$9.y as x, Store$15.y as y, ref$10.y as h
          FROM Store as Store$15, varPointsTo as ref$9, varPointsTo as ref$10
          WHERE Store$15.x = ref$9.x AND Store$15.h = ref$10.x)))
SELECT COUNT(1)
FROM varPointsTo as recref$0
WHERE recref$0.x = 'r'
    """

  def expectedQueryPattern: String =
    appendixSQLString
      .replaceAll("\'", "\"")
      .replaceAll("(?m)^CREATE TABLE.*", "")
      .replaceAll("varPointsTo", "recursive\\$A")
      .replaceAll("heapPointsTo", "recursive\\$B")
}

class JPTAppendixTest extends SQLStringQueryTest[EmptyDB, (x: String, y: String)] {
  def testDescription: String = "JPT"
  def query() =
    val options = (
      constructorFreedom = RestrictedConstructors(),
      monotonicity = Monotone(),
      category = BagResult(),
      linearity = NonLinear()
    )
    val newT = Table[(x: String, y: String)]("New")
    val assign = Table[(x: String, y: String)]("Assign")
    val store = Table[(x: String, y: String, h: String)]("Store")
    val loadT = Table[(x: String, y: String, h: String)]("LoadT")

    val baseVPT = newT.map(a => (x = a.x, y = a.y).toRow)
    val baseHPT = Table[(x: String, y: String, h: String)]("BaseHPT")

    val pt =
      customFix(options)((baseVPT, baseHPT))((varPointsTo, heapPointsTo) =>
        val vpt = assign.flatMap(a =>
          varPointsTo.filter(p => a.y == p.x).map(p =>
            (x = a.x, y = p.y).toRow
          )
        ).unionAll(
          loadT.flatMap(l =>
            heapPointsTo.flatMap(hpt =>
              varPointsTo
                .filter(vpt => l.y == vpt.x && l.h == hpt.y && vpt.y == hpt.x)
                .map(pt2 =>
                  (x = l.x, y = hpt.h).toRow))))
        val hpt = store.flatMap(s =>
          varPointsTo.flatMap(vpt1 =>
            varPointsTo
              .filter(vpt2 => s.x == vpt1.x && s.h == vpt2.x)
              .map(vpt2 =>
                (x = vpt1.y, y = s.y, h = vpt2.y).toRow)))
        (vpt, hpt)
      )
    pt._1

  def appendixSQLString: String =
    """
CREATE TABLE New ( x TEXT, y TEXT );
CREATE TABLE Assign ( x TEXT, y TEXT );
CREATE TABLE LoadT ( x TEXT, y TEXT, h TEXT );
CREATE TABLE Store ( x TEXT, y TEXT, h TEXT );
CREATE TABLE BaseHPT ( x TEXT, y TEXT, h TEXT );
WITH RECURSIVE
    varPointsTo AS
        ((SELECT New$2.x as x, New$2.y as y
          FROM New as New$2)
            UNION ALL
        ((SELECT Assign$4.x as x, ref$2.y as y
          FROM Assign as Assign$4, varPointsTo as ref$2
          WHERE Assign$4.y = ref$2.x)
            UNION ALL
        (SELECT LoadT$7.x as x, ref$5.h as y
         FROM LoadT as LoadT$7, heapPointsTo as ref$5, varPointsTo as ref$6
         WHERE LoadT$7.y = ref$6.x AND LoadT$7.h = ref$5.y AND ref$6.y = ref$5.x))),
    heapPointsTo AS
        ((SELECT *
          FROM BaseHPT as BaseHPT$13)
            UNION ALL
        ((SELECT ref$9.y as x, Store$15.y as y, ref$10.y as h
          FROM Store as Store$15, varPointsTo as ref$9, varPointsTo as ref$10
          WHERE Store$15.x = ref$9.x AND Store$15.h = ref$10.x)))
SELECT *
FROM varPointsTo as recref$0
"""

  def expectedQueryPattern: String =
    appendixSQLString
      .replaceAll("\'", "\"")
      .replaceAll("(?m)^CREATE TABLE.*", "")
      .replaceAll("varPointsTo", "recursive\\$A")
      .replaceAll("heapPointsTo", "recursive\\$B")
}

class PartyAppendixTest extends SQLStringQueryTest[EmptyDB, (person: String)] {
  def testDescription: String = "Party"
  def query() =
    val options = (
      constructorFreedom = NonRestrictedConstructors(),
      monotonicity = NonMonotone(),
      category = BagResult(),
      linearity = Linear()
    )
    val organizers = Table[(orgName: String)]("Organizers")
    val friend = Table[(pName: String, fName: String)]("Friends")
    val counts = Table[(fName: String, nCount: Int)]("Counts")

    val baseAttend = organizers.map(o => (person = o.orgName).toRow)

    val result =
      unrestrictedBagFix((baseAttend, counts))((attendR, cntFriendsR) =>
//      customFix(options)(baseAttend, counts)((attendR, cntFriendsR) =>
        val recurAttend = cntFriendsR
          .filter(cf => cf.nCount > 2)
          .map(cf => (person = cf.fName).toRow)

        val recurCntFriends = friend
          .aggregate(friends =>
            attendR
              .filter(att => att.person == friends.fName)
              .aggregate(att => (fName = friends.pName, nCount = count(friends.fName)).toGroupingRow)
          ).groupBySource(f => (name = f._1.pName).toRow)
        (recurAttend, recurCntFriends))
    result._1

  def appendixSQLString: String =
    """
CREATE TABLE Organizers ( OrgName VARCHAR(100) PRIMARY KEY );
CREATE TABLE Friends ( Pname VARCHAR(100), Fname VARCHAR(100) );
CREATE TABLE Counts ( fName VARCHAR(100), nCount INT );
WITH RECURSIVE
    attendR AS
        ((SELECT Organizers$65.orgName as person
          FROM Organizers as Organizers$65)
            UNION ALL
        ((SELECT ref$35.fName as person
          FROM cntFriendsR as ref$35
          WHERE ref$35.nCount > 2))),
    cntFriendsR AS
        ((SELECT *
          FROM Counts as Counts$69)
            UNION ALL
        ((SELECT Friends$71.pName as fName, COUNT(Friends$71.fName) as nCount
          FROM Friends as Friends$71, attendR as ref$38
          WHERE ref$38.person = Friends$71.fName
          GROUP BY Friends$71.pName)))
SELECT * FROM attendR as recref$5
"""

  def expectedQueryPattern: String =
    appendixSQLString
      .replaceAll("\'", "\"")
      .replaceAll("(?m)^CREATE TABLE.*", "")
      .replaceAll("attendR", "recursive\\$A")
      .replaceAll("cntFriendsR", "recursive\\$B")
}

class CBAAppendixTest extends SQLStringAggregationTest[EmptyDB, Int] {
  def testDescription: String = "CBA"
  def query() =
    val options = (
      constructorFreedom = RestrictedConstructors(),
      monotonicity = Monotone(),
      category = BagResult(),
      linearity = NonLinear()
    )
    val term = Table[(x: Int, y: String, z: Int)]("Term")
    val lits = Table[(x: Int, y: String)]("Lits")
    val vars = Table[(x: Int, y: String)]("Vars")
    val abs = Table[(x: Int, y: Int, z: Int)]("Abs")
    val app = Table[(x: Int, y: Int, z: Int)]("App")
    val baseData = Table[(x: Int, y: String)]("BaseData")
    val baseCtrl = Table[(x: Int, y: Int)]("BaseCtrl")
    val dataTermBase = term.flatMap(t => lits.filter(l => l.x == t.z && t.y == StringLit("Lit")).map(l => (x = t.x, y = l.y).toRow))
    val dataVarBase = baseData
    val ctrlTermBase = term.filter(t => t.y == StringLit("Abs")).map(t => (x = t.x, y = t.z).toRow)
    val ctrlVarBase = baseCtrl
    val result =
      customFix(options)((dataTermBase, dataVarBase, ctrlTermBase, ctrlVarBase))(
        (dataTermR, dataVarR, ctrlTermR, ctrlVarR) =>
          val dt1 =
            for
              t <- term
              dv <- dataVarR
              if t.y == "Var" && t.z == dv.x
            yield (x = t.x, y = dv.y).toRow
          val dt2 =
            for
              t <- term
              dt <- dataTermR
              ct <- ctrlTermR
              ab <- abs
              ap <- app
              if t.y == "App" && t.z == ap.x && dt.x == ab.z && ct.x == ap.y && ct.y == ab.x
            yield (x = t.x, y = dt.y).toRow
          val dv =
            for
              ct <- ctrlTermR
              dt <- dataTermR
              ab <- abs
              ap <- app
              if ct.x == ap.y && ct.y == ab.x && dt.x == ap.z
            yield (x = ab.y, y = dt.y).toRow
          val ct1 =
            for
              t <- term
              cv <- ctrlVarR
              if t.y == "Var" && t.z == cv.x
            yield (x = t.x, y = cv.y).toRow
          val ct2 =
            for
              t <- term
              ct1 <- ctrlTermR
              ct2 <- ctrlTermR
              ab <- abs
              ap <- app
              if t.y == "App" && t.z == ap.x && ct1.x == ab.z && ct2.x == ap.y && ct2.y == ab.x
            yield (x = t.x, y = ct1.y).toRow
          val cv =
            for
              ct1 <- ctrlTermR
              ct2 <- ctrlTermR
              ab <- abs
              ap <- app
              if ct1.x == ap.y && ct1.y == ab.x && ct2.x == ap.z
            yield (x = ab.y, y = ct2.y).toRow
          (dt1.unionAll(dt2), dv, ct1.unionAll(ct2), cv)
        )
    result._1.distinct.size

  def appendixSQLString: String =
    """
CREATE TABLE Term ( x INTEGER, y TEXT, z INTEGER );
CREATE TABLE Lits ( x INTEGER, y TEXT );
CREATE TABLE Vars ( x INTEGER, y TEXT );
CREATE TABLE Abs ( x INTEGER, y INTEGER, z INTEGER );
CREATE TABLE App ( x INTEGER, y INTEGER, z INTEGER );
CREATE TABLE BaseCtrl ( x INTEGER, y INTEGER );
CREATE TABLE BaseData ( x INTEGER, y TEXT );
WITH RECURSIVE
    dataTermR AS
        ((SELECT Term$43.x as x, Lits$44.y as y
        FROM Term as Term$43, Lits as Lits$44
          WHERE Lits$44.x = Term$43.z AND Term$43.y = 'Lit') UNION ALL ((SELECT Term$47.x as x, ref$28.y as y
          FROM Term as Term$47, dataVarR as ref$28
          WHERE Term$47.y = 'Var' AND Term$47.z = ref$28.x)
              UNION ALL (SELECT Term$50.x as x, ref$31.y as y
          FROM Term as Term$50, dataTermR as ref$31, ctrlTermR as ref$32, Abs as Abs$51, App as App$52
          WHERE Term$50.y = 'App' AND Term$50.z = App$52.x AND ref$31.x = Abs$51.z AND ref$32.x = App$52.y AND ref$32.y = Abs$51.x))),
    dataVarR AS
        ((SELECT *
          FROM BaseData as BaseData$60)
              UNION ALL
        ((SELECT Abs$62.y as x, ref$37.y as y
          FROM ctrlTermR as ref$36, dataTermR as ref$37, Abs as Abs$62, App as App$63
          WHERE ref$36.x = App$63.y AND ref$36.y = Abs$62.x AND ref$37.x = App$63.z))),
    ctrlTermR AS
        ((SELECT Term$69.x as x, Term$69.z as y
          FROM Term as Term$69
          WHERE Term$69.y = 'Abs')
              UNION ALL
        ((SELECT Term$71.x as x, ref$42.y as y
          FROM Term as Term$71, ctrlVarR as ref$42
          WHERE Term$71.y = 'Var' AND Term$71.z = ref$42.x)
              UNION ALL
         (SELECT Term$74.x as x, ref$45.y as y
          FROM Term as Term$74, ctrlTermR as ref$45, ctrlTermR as ref$46, Abs as Abs$75, App as App$76
          WHERE Term$74.y = 'App' AND Term$74.z = App$76.x AND ref$45.x = Abs$75.z AND ref$46.x = App$76.y AND ref$46.y = Abs$75.x))),
    ctrlVarR AS
        ((SELECT *
          FROM BaseCtrl as BaseCtrl$84)
              UNION ALL
         ((SELECT Abs$86.y as x, ref$51.y as y
          FROM ctrlTermR as ref$50, ctrlTermR as ref$51, Abs as Abs$86, App as App$87
          WHERE ref$50.x = App$87.y AND ref$50.y = Abs$86.x AND ref$51.x = App$87.z)))
SELECT DISTINCT COUNT(1) FROM dataTermR as recref$3
"""

  def expectedQueryPattern: String =
    appendixSQLString
      .replaceAll("\'", "\"")
      .replaceAll("(?m)^CREATE TABLE.*", "")
      .replaceAll("dataTermR", "recursive\\$A")
      .replaceAll("dataVarR", "recursive\\$B")
      .replaceAll("ctrlTermR", "recursive\\$C")
      .replaceAll("ctrlVarR", "recursive\\$D")
}

class SSSPAppendixTest extends SQLStringQueryTest[EmptyDB, (dst: String, cost: Int)] {
  def testDescription: String = "SSSP"
  def query() =
    val options = (
      constructorFreedom = NonRestrictedConstructors(),
      monotonicity = Monotone(),
      category = SetResult(),
      linearity = Linear()
    )
    val base = Table[(dst: String, cost: Int)]("Base")
    val edge = Table[(src: String, dst: String, cost: Int)]("Edge")
    base.customFix(options)(pathR =>
        edge.flatMap(edge =>
          pathR
            .filter(s => s.dst == edge.src)
            .map(s => (dst = edge.dst, cost = s.cost + edge.cost).toRow)
        ).distinct
      )
      .aggregate(s => (dst = s.dst, cost = min(s.cost)).toGroupingRow)
      .groupBySource(s => (dst = s._1.dst).toRow)

  def appendixSQLString: String =
    """
CREATE TABLE Edge ( src INT, dst INT, cost INT );
CREATE TABLE Base ( dst INT, cost INT );
WITH RECURSIVE
    pathR AS
        ((SELECT *
          FROM Base as Base$62)
            UNION
        ((SELECT Edge$64.dst as dst, ref$29.cost + Edge$64.cost as cost
          FROM Edge as Edge$64, pathR as ref$29
          WHERE ref$29.dst = Edge$64.src)))
SELECT recref$5.dst as dst, MIN(recref$5.cost) as cost
FROM pathR as recref$5
GROUP BY recref$5.dst
"""

  def expectedQueryPattern: String =
    appendixSQLString
      .replaceAll("\'", "\"")
      .replaceAll("(?m)^CREATE TABLE.*", "")
      .replaceAll("pathR", "recursive\\$A")
}

class AncestryAppendixTest extends SQLStringQueryTest[EmptyDB, (name: String)] {
  def testDescription: String = "Ancestry"
  def query() =
    val options = (
      constructorFreedom = NonRestrictedConstructors(),
      monotonicity = Monotone(),
      category = SetResult(),
      linearity = Linear()
    )
    val parents = Table[(parent: String, child: String)]("Parents")
    val base = parents
      .filter(p => p.parent == "A")
      .map(e => (name = e.child, gen = IntLit(1)).toRow)

    base.customFix(options)(genR =>
        parents.flatMap(parent =>
          genR
            .filter(g => parent.parent == g.name)
            .map(g => (name = parent.child, gen = g.gen + 1).toRow)
        ).distinct)
      .filter(g => g.gen == 2)
      .map(g => (name = g.name).toRow)

  def appendixSQLString: String =
    """
CREATE TABLE Parents ( parent TEXT, child TEXT );
WITH RECURSIVE
    genR AS
        ((SELECT Parents$162.child as name, 1 as gen
          FROM Parents as Parents$162
          WHERE Parents$162.parent = 'A')
            UNION
        ((SELECT Parents$164.child as name, ref$86.gen + 1 as gen
          FROM Parents as Parents$164, genR as ref$86
          WHERE Parents$164.parent = ref$86.name)))
SELECT recref$14.name as name
FROM genR as recref$14
WHERE recref$14.gen = 2
"""

  def expectedQueryPattern: String =
    appendixSQLString
      .replaceAll("\'", "\"")
      .replaceAll("(?m)^CREATE TABLE.*", "")
      .replaceAll("genR", "recursive\\$A")
}

class APTAppendixTest extends SQLStringQueryTest[EmptyDB, (x: String, y: String)] {
  def testDescription: String = "APT"
  def query() =
    val options = (
      constructorFreedom = RestrictedConstructors(),
      monotonicity = Monotone(),
      category = SetResult(),
      linearity = NonLinear()
    )
    val addressOf = Table[(x: String, y: String)]("AddressOf")
    val assign = Table[(x: String, y: String)]("Assign")
    val loadT = Table[(x: String, y: String)]("LoadT")
    val store = Table[(x: String, y: String)]("Store")
    val base = addressOf.map(a => (x = a.x, y = a.y).toRow)

    base.customFix(options)(pointsToR =>
      assign.flatMap(a =>
          pointsToR.filter(p => a.y == p.x).map(p =>
            (x = a.x, y = p.y).toRow))
        .union(loadT.flatMap(l =>
          pointsToR.flatMap(pt1 =>
            pointsToR
              .filter(pt2 => l.y == pt1.x && pt1.y == pt2.x)
              .map(pt2 =>
                (x = l.x, y = pt2.y).toRow))))
        .union(store.flatMap(s =>
          pointsToR.flatMap(pt1 =>
            pointsToR
              .filter(pt2 => s.x == pt1.x && s.y == pt2.x)
              .map(pt2 =>
                (x = pt1.y, y = pt2.y).toRow)))))

  def appendixSQLString: String =
    """
CREATE TABLE addressOf ( x TEXT, y TEXT );
CREATE TABLE assign ( x TEXT, y TEXT );
CREATE TABLE loadT ( x TEXT, y TEXT );
CREATE TABLE store ( x TEXT, y TEXT );
WITH RECURSIVE
  pointsToR AS
    ((SELECT AddressOf$246.x as x, AddressOf$246.y as y
      FROM AddressOf as AddressOf$246)
        UNION
    ((SELECT Assign$248.x as x, ref$126.y as y
      FROM Assign as Assign$248, pointsToR as ref$126
      WHERE Assign$248.y = ref$126.x)
        UNION
     (SELECT LoadT$251.x as x, ref$130.y as y
      FROM LoadT as LoadT$251, pointsToR as ref$129, pointsToR as ref$130
      WHERE LoadT$251.y = ref$129.x AND ref$129.y = ref$130.x)
        UNION
     (SELECT ref$133.y as x, ref$134.y as y
      FROM Store as Store$256, pointsToR as ref$133, pointsToR as ref$134
      WHERE Store$256.x = ref$133.x AND Store$256.y = ref$134.x)))
SELECT * FROM pointsToR as recref$21
"""

  def expectedQueryPattern: String =
    appendixSQLString
      .replaceAll("\'", "\"")
      .replaceAll("(?m)^CREATE TABLE.*", "")
      .replaceAll("pointsToR", "recursive\\$A")
}


class APSPAppendixTest extends SQLStringQueryTest[EmptyDB, (src: Int, dst: Int, cost: Int)] {
  def testDescription: String = "APSP"
  def query() =
    val options = (
      constructorFreedom = NonRestrictedConstructors(),
      monotonicity = NonMonotone(),
      category = SetResult(),
      linearity = NonLinear()
    )
    val edge = Table[(src: Int, dst: Int, cost: Int)]("Edge")
    val baseEdges = edge
      .aggregate(e =>
        (src = e.src, dst = e.dst, cost = min(e.cost)).toGroupingRow)
      .groupBySource(e =>
        (src = e._1.src, dst = e._1.dst).toRow)

    val asps =
//      base.customFix(options)(pathR =>
      baseEdges.unrestrictedFix(pathR =>
        pathR.aggregate(p =>
            pathR
              .filter(e =>
                p.dst == e.src)
              .aggregate(e =>
                (src = p.src, dst = e.dst, cost = min(p.cost + e.cost)).toGroupingRow))
          .groupBySource(p =>
            (g1 = p._1.src, g2 = p._2.dst).toRow).distinct
      )
    asps
      .aggregate(a =>
        (src = a.src, dst = a.dst, cost = min(a.cost)).toGroupingRow)
      .groupBySource(p =>
        (g1 = p._1.src, g2 = p._1.dst).toRow)

  def appendixSQLString: String =
    """
CREATE TABLE edge ( src INT, dst INT, cost INT );
WITH RECURSIVE
  pathR AS
    ((SELECT Edge$1.src as src, Edge$1.dst as dst, MIN(Edge$1.cost) as cost
      FROM Edge as Edge$1
      GROUP BY Edge$1.src, Edge$1.dst)
        UNION
    ((SELECT ref$2.src as src, ref$3.dst as dst, MIN(ref$2.cost + ref$3.cost) as cost
      FROM pathR as ref$2, pathR as ref$3
      WHERE ref$2.dst = ref$3.src GROUP BY ref$2.src, ref$3.dst)))
SELECT recref$0.src as src, recref$0.dst as dst, MIN(recref$0.cost) as cost
FROM pathR as recref$0
GROUP BY recref$0.src, recref$0.dst
"""

  def expectedQueryPattern: String =
    appendixSQLString
      .replaceAll("\'", "\"")
      .replaceAll("(?m)^CREATE TABLE.*", "")
      .replaceAll("pathR", "recursive\\$A")
}


class TCAppendixTest extends SQLStringQueryTest[EmptyDB, (startNode: Int, endNode: Int, path: List[Int])] {
  def testDescription: String = "TC"
  def query() =
    val options = (
      constructorFreedom = NonRestrictedConstructors(),
      monotonicity = Monotone(),
      category = BagResult(),
      linearity = Linear()
    )
    val edge = Table[(x: Int, y: Int)]("Edge")
    val pathBase = edge
      .filter(p => p.x == 1)
      .map(e => (startNode = e.x, endNode = e.y, path = List(e.x, e.y).toExpr).toRow)

    pathBase
      .customFix(options)(pathR =>
        pathR.flatMap(p =>
          edge
            .filter(e => e.x == p.endNode && !p.path.contains(e.y))
            .map(e =>
              (startNode = p.startNode, endNode = e.y, path = p.path.append(e.y)).toRow)))

  def appendixSQLString: String =
    """
CREATE TABLE edge ( x INTEGER, y INTEGER );
WITH RECURSIVE
  pathR AS
    ((SELECT Edge$277.x as startNode, Edge$277.y as endNode, [Edge$277.x, Edge$277.y] as path
      FROM Edge as Edge$277
      WHERE Edge$277.x = 1)
        UNION ALL
    ((SELECT ref$147.startNode as startNode, Edge$279.y as endNode, list_append(ref$147.path, Edge$279.y) as path
      FROM pathR as ref$147, Edge as Edge$279
      WHERE Edge$279.x = ref$147.endNode AND NOT list_contains(ref$147.path, Edge$279.y))))
SELECT * FROM pathR as recref$23
"""

  def expectedQueryPattern: String =
    appendixSQLString
      .replaceAll("\'", "\"")
      .replaceAll("(?m)^CREATE TABLE.*", "")
      .replaceAll("pathR", "recursive\\$A")
}

class BOMAppendixTest extends SQLStringQueryTest[EmptyDB, (part: String, max: Int)] {
  def testDescription: String = "BOM"

  def query() =
    val options = (
      constructorFreedom = RestrictedConstructors(),
      monotonicity = Monotone(),
      category = BagResult(),
      linearity = Linear()
    )
    val assbl = Table[(part: String, spart: String)]("Assbl")
    val basic = Table[(part: String, days: Int)]("Basic")
    val result = basic
      .customFix(options)(waitForR =>
        assbl.flatMap(assbl =>
          waitForR
            .filter(wf => assbl.spart == wf.part)
            .map(wf => (part = assbl.part, days = wf.days).toRow)))
    result
      .aggregate(wf => (part = wf.part, max = max(wf.days)).toGroupingRow)
      .groupBySource(wf => (part = wf._1.part).toRow)

  def appendixSQLString: String =
    """
CREATE TABLE Assbl ( part TEXT, spart TEXT );
CREATE TABLE basic ( part TEXT, days INT );
WITH RECURSIVE
  waitForR AS
    ((SELECT * FROM
      Basic as Basic$55)
        UNION ALL
    ((SELECT Assbl$57.part as part, ref$32.days as days
      FROM Assbl as Assbl$57, waitForR as ref$32
      WHERE Assbl$57.spart = ref$32.part)))
SELECT recref$5.part as part, MAX(recref$5.days) as max
FROM waitForR as recref$5
GROUP BY recref$5.part
"""

  def expectedQueryPattern: String =
    appendixSQLString
      .replaceAll("\'", "\"")
      .replaceAll("(?m)^CREATE TABLE.*", "")
      .replaceAll("waitForR", "recursive\\$A")
}

class OrbitsAppendixTest extends SQLStringQueryTest[EmptyDB, (x: String, y: String)] {
  def testDescription: String = "Orbits"

  def query() =
    val options = (
      constructorFreedom = RestrictedConstructors(),
      monotonicity = Monotone(),
      category = BagResult(),
      linearity = NonLinear()
    )
    val base = Table[(x: String, y: String)]("Base")
    val orbits =
      base.customFix(options)(orbitsR =>
        orbitsR.flatMap(p =>
          orbitsR
            .filter(e => p.y == e.x)
            .map(e => (x = p.x, y = e.y).toRow)
        )
      )
    orbits match
      case Query.MultiRecursive(_, _, orbitsRef) =>
        orbits.filter(o =>
          orbitsRef
            .flatMap(o1 =>
              orbitsRef
                .filter(o2 => o1.y == o2.x)
                .map(o2 => (x = o1.x, y = o2.y).toRow))
            .filter(io => o.x == io.x && o.y == io.y)
            .nonEmpty
        )

  def appendixSQLString: String =
    """
CREATE TABLE base ( x TEXT, y TEXT );
WITH RECURSIVE
  orbitsR AS
    ((SELECT *
      FROM Base as Base$83)
        UNION ALL
    ((SELECT ref$42.x as x, ref$43.y as y
      FROM orbitsR as ref$42, orbitsR as ref$43
      WHERE ref$42.y = ref$43.x)))
SELECT *
FROM orbitsR as recref$8
WHERE EXISTS
  (SELECT *
   FROM
    (SELECT ref$46.x as x, ref$47.y as y
     FROM orbitsR as ref$46, orbitsR as ref$47
     WHERE ref$46.y = ref$47.x) as subquery$91
   WHERE recref$8.x = subquery$91.x AND recref$8.y = subquery$91.y)
"""

  def expectedQueryPattern: String =
    appendixSQLString
      .replaceAll("\'", "\"")
      .replaceAll("(?m)^CREATE TABLE.*", "")
      .replaceAll("orbitsR", "recursive\\$A")
}

class DataflowAppendixTest extends SQLStringQueryTest[EmptyDB, (r: String, w: String)] {
  def testDescription: String = "Dataflow"

  def query() =
    val options = (
      constructorFreedom = RestrictedConstructors(),
      monotonicity = Monotone(),
      category = BagResult(),
      linearity = NonLinear()
    )
    val readOp = Table[(opN: String, varN: String)]("ReadOp")
    val writeOp = Table[(opN: String, varN: String)]("WriteOp")
    val jumpOp = Table[(a: String, b: String)]("JumpOp")
    jumpOp
      .customFix(options)(flowR =>
        flowR.flatMap(f1 =>
          flowR.filter(f2 => f1.b == f2.a).map(f2 =>
            (a = f1.a, b = f2.b).toRow)))
      .flatMap(f =>
        readOp.flatMap(r =>
          writeOp
            .filter(w => w.opN == f.a && w.varN == r.varN && f.b == r.opN)
            .map(w => (r = r.opN, w = w.opN).toRow)))

  def appendixSQLString: String =
    """
CREATE TABLE ReadOp ( opN TEXT, varN TEXT );
CREATE TABLE WriteOp ( opN TEXT, varN TEXT );
CREATE TABLE JumpOp ( a TEXT, b TEXT );
WITH RECURSIVE
  flowR AS
    ((SELECT * FROM
      JumpOp as JumpOp$119)
        UNION ALL
    ((SELECT ref$64.a as a, ref$65.b as b
      FROM flowR as ref$64, flowR as ref$65
      WHERE ref$64.b = ref$65.a)))
SELECT ReadOp$126.opN as r, WriteOp$127.opN as w
FROM flowR as recref$12, ReadOp as ReadOp$126, WriteOp as WriteOp$127
WHERE WriteOp$127.opN = recref$12.a AND WriteOp$127.varN = ReadOp$126.varN AND recref$12.b = ReadOp$126.opN
"""

  def expectedQueryPattern: String =
    appendixSQLString
      .replaceAll("\'", "\"")
      .replaceAll("(?m)^CREATE TABLE.*", "")
      .replaceAll("flowR", "recursive\\$A")
}