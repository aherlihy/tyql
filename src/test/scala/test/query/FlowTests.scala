package test.query.flow
import test.{SQLStringQueryTest, SQLStringAggregationTest}
import test.query.{AllCommerceDBs, ShippingInfo, commerceDBs, Buyer}
import tyql.*

import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions
import java.time.LocalDate
import tyql.Expr.sum

class FlowForTest1 extends SQLStringQueryTest[AllCommerceDBs, (bName: String, bId: Int)] {
  def testDescription = "Flow: project tuple, 1 nest, for comprehension"
  def query() =
    for
      b <- testDB.tables.buyers
    yield (bName = b.name, bId = b.id).toRow

  def expectedQueryPattern = "SELECT buyers$A.name as bName, buyers$A.id as bId FROM buyers as buyers$A"
}

/*class FlowForTest2 extends SQLStringQueryTest[AllCommerceDBs, (name: String, shippingDate: LocalDate)] {
  def testDescription = "Flow: project tuple, 2 nest, for comprehension"
  def query() =
    for
      b <- testDB.tables.buyers
      si <- testDB.tables.shipInfos
    yield (name = b.name, shippingDate = si.shippingDate).toRow

  def sqlString = "SELECT r0.name, r1.shippingDate FROM buyers r0, shipInfos r1"
}
*/

class FlowForTest3 extends SQLStringQueryTest[AllCommerceDBs, String] {
  def testDescription = "Flow: single field, 1 nest, for comprehension"
  def query() =
    for
      b <- testDB.tables.buyers
    yield b.name

  def expectedQueryPattern = "SELECT buyers$A.name FROM buyers as buyers$A"
}
/*
class FlowForTest4 extends SQLStringQueryTest[AllCommerceDBs, String] {
  def testDescription = "Flow: single field, 2 nest, for comprehension"
  def query() =
    for
      b <- testDB.tables.buyers
      si <- testDB.tables.shipInfos
    yield b.name

  def sqlString = "SELECT b.name FROM buyers b, shipInfos si"
}
*/
class FlowForIfTest5 extends SQLStringQueryTest[AllCommerceDBs, (bName: String, bId: Int)] {
  def testDescription = "Flow: project tuple, 1 nest, for comprehension + if"

  def query() =
    for
      b <- testDB.tables.buyers
      if b.id > 1
    yield (bName = b.name, bId = b.id).toRow

  def expectedQueryPattern = "SELECT buyers$A.name as bName, buyers$A.id as bId FROM buyers as buyers$A WHERE buyers$A.id > 1"
}

class FlowForAggregateTest extends SQLStringQueryTest[AllCommerceDBs, (pName: String, sumP: Double)] {
  def testDescription = "Flow: for comprehension with agg"

  def query() =
    for
      p <- testDB.tables.products
    yield (pName = p.name, sumP = sum(p.price))

  def expectedQueryPattern = "SELECT product$A.name as pName, SUM(product$A.price) as sumP FROM product as product$A"
}

class FlowMapTest1 extends SQLStringQueryTest[AllCommerceDBs, (bName: String, bId: Int)] {
  def testDescription = "Flow: project tuple, 1 nest, map"
  def query() =
    testDB.tables.buyers.map(b =>
      (bName = b.name, bId = b.id).toRow
    )

  def expectedQueryPattern = "SELECT buyers$A.name as bName, buyers$A.id as bId FROM buyers as buyers$A"
}
/*

class FlowFlatMapTest2 extends SQLStringQueryTest[AllCommerceDBs, (name: String, shippingDate: LocalDate)] {
  def testDescription = "Flow: project tuple, 2 nest, flatmap+map"

  def query() =
    testDB.tables.buyers.flatMap(b =>
      testDB.tables.shipInfos.map(si =>
        (name = b.name, shippingDate = si.shippingDate).toRow
      )
    )

  def sqlString = "SELECT r0.name, r1.shippingDate FROM buyers r0, shipInfos r1"
}

class FlowFlatMapTest3 extends SQLStringQueryTest[AllCommerceDBs, (name: String, shippingDate1: LocalDate, shippingDate2: LocalDate)] {
  def testDescription = "Flow: project tuple, 3 nest, flatMap + flatmap+map"

  def query() =
    testDB.tables.shipInfos.flatMap(si1 =>
      testDB.tables.buyers.flatMap(b =>
        testDB.tables.shipInfos.map(si2 =>
          (name = b.name, shippingDate1 = si1.shippingDate, shippingDate2 = si2.shippingDate).toRow
        )
      )
    )

  def sqlString = ""
}
*/

class FlowMapFilterWithTest extends SQLStringQueryTest[AllCommerceDBs, (bName: String, bId: Int)] {
  def testDescription = "Flow: project tuple, 1 nest, map + filterWith"

  def query() =
    testDB.tables.buyers.withFilter(_.id > 1).map(b =>
      (bName = b.name, bId = b.id).toRow
    )
  def expectedQueryPattern = "SELECT buyers$A.name as bName, buyers$A.id as bId FROM buyers as buyers$A WHERE buyers$A.id > 1"
}

class FlowMapFilterTest extends SQLStringQueryTest[AllCommerceDBs, (bName: String, bId: Int)] {
  def testDescription = "Flow: project tuple, 1 nest, map + filter"

  def query() =
    testDB.tables.buyers.filter(_.id > 1).map(b =>
      (bName = b.name, bId = b.id).toRow
    )
  def expectedQueryPattern = "SELECT buyers$A.name as bName, buyers$A.id as bId FROM buyers as buyers$A WHERE buyers$A.id > 1"
}

class FlowMapSubsequentFilterTest extends SQLStringQueryTest[AllCommerceDBs, (bName: String, bId: Int)] {
  def testDescription = "Flow: project tuple, 1 nest, map + filter x 3"

  def query() =
    testDB.tables.buyers
      .filter(_.id > 1)
      .filter(_.id > 10) // ignore nonsensical constraints
      .filter(_.id > 100)
      .map(b =>
      (bName = b.name, bId = b.id).toRow
    )

  def expectedQueryPattern = "SELECT buyers$A.name as bName, buyers$A.id as bId FROM buyers as buyers$A WHERE buyers$A.id > 100 AND buyers$A.id > 10 AND buyers$A.id > 1"
}

class FlowSubsequentMapTest extends SQLStringQueryTest[AllCommerceDBs, (bName3: String, bId3: Int)] {
  def testDescription = "Flow: project tuple, 1 nest, map x 3"

  def query() =
    testDB.tables.buyers
      .map(b =>
        (bName = b.name, bId = b.id).toRow
      )
      .map(b =>
        (bName2 = b.bName, bId2 = b.bId).toRow
      )
      .map(b =>
        (bName3 = b.bName2, bId3 = b.bId2).toRow
      )
  def expectedQueryPattern = "SELECT subquery$A.bName2 as bName3, subquery$A.bId2 as bId3 FROM (SELECT subquery$B.bName as bName2, subquery$B.bId as bId2 FROM (SELECT buyers$C.name as bName, buyers$C.id as bId FROM buyers as buyers$C) as subquery$B) as subquery$A"
}

class FlowAllSubsequentFilterTest extends SQLStringQueryTest[AllCommerceDBs, Buyer] {
  def testDescription = "Flow: all, 1 nest, filter x 3"

  def query() =
    testDB.tables.buyers
      .filter(_.id > 1)
      .filter(_.id > 10) // ignore nonsensical constraints
      .filter(_.id > 100)

  def expectedQueryPattern = "SELECT * FROM buyers as buyers$A WHERE buyers$A.id > 100 AND buyers$A.id > 10 AND buyers$A.id > 1"
}

class FlowMapFilterTest2 extends SQLStringQueryTest[AllCommerceDBs, (bName: String, bId: Int)] {
  def testDescription = "Flow: project tuple, 1 nest, filter after map"

  def query() =
    testDB.tables.buyers.map(b =>
      (bName = b.name, bId = b.id).toRow
    ).filter(_.bId > 1) // for now generate subquery
  def expectedQueryPattern = "SELECT * FROM (SELECT buyers$A.name as bName, buyers$A.id as bId FROM buyers as buyers$A) as subquery$B WHERE subquery$B.bId > 1"
}

class FlowMapAggregateTest extends SQLStringQueryTest[AllCommerceDBs, Int] {
  def testDescription = "Flow: map on aggregate"

  def query() =
      testDB.tables.shipInfos.map(si =>
        sum(si.buyerId)
      )

  def expectedQueryPattern = "SELECT SUM(shippingInfo$A.buyerId) FROM shippingInfo as shippingInfo$A"
}
/*
class FlowMapAggregateTest2 extends SQLStringAggregationTest[AllCommerceDBs, Int] {
  def testDescription = "Flow: flatMap on aggregate"

  def query() =
    testDB.tables.shipInfos.flatMap(si =>
      sum(si.buyerId)
    )

  def expectedQueryPattern = ""
}


class FlowMapAggregateTest3 extends SQLStringQueryTest[AllCommerceDBs, Int] {
  def testDescription = "Flow: 2 nest, map+flatMap should not fail because aggregation is Expr"
  def query() =
    testDB.tables.buyers.map(b =>
      testDB.tables.shipInfos.flatMap(si =>
        sum(si.buyerId)
      )
    )

  def sqlString = ""
}

class FlowMapAggregateTest4 extends SQLStringAggregationTest[AllCommerceDBs, Int] {
  def testDescription = "Flow: 2 nest, flatMap+flatMap should not fail because aggregation is Expr"
  def query() =
    testDB.tables.buyers.flatMap(b =>
      testDB.tables.shipInfos.flatMap(si => // silly but correct syntax, equivalent to map + flatMap
        sum(si.buyerId)
      )
    )

  def sqlString = ""
}

*/
class FlowMapAggregateTest6 extends SQLStringQueryTest[AllCommerceDBs, (sum: Int)] {
  def testDescription = "Flow: project + map on aggregate"

  def query() =
    testDB.tables.shipInfos.map(si =>
      (sum = sum(si.buyerId)).toRow
    )

  def expectedQueryPattern = "SELECT SUM(shippingInfo$A.buyerId) as sum FROM shippingInfo as shippingInfo$A"
}

class FlowMapAggregateConvertedTest extends SQLStringQueryTest[AllCommerceDBs, (sum: Int)] {
  def testDescription = "Flow: project tuple, version of map that calls toRow for conversion for aggregate"

  def query() =
    testDB.tables.shipInfos.map(si =>
      (sum = sum(si.buyerId))
    )

  def expectedQueryPattern = "SELECT SUM(shippingInfo$A.buyerId) as sum FROM shippingInfo as shippingInfo$A"
}
/*
class FlowMapAggregateTest7 extends SQLStringAggregationTest[AllCommerceDBs, (sum: Int)] {
  def testDescription = "Flow: project + flatMap on aggregate"

  def query() =
    testDB.tables.shipInfos.flatMap(si =>
      (sum = sum(si.buyerId)).toRow
    )

  def expectedQueryPattern = ""
}
*/
class FlowMapConvertedTest extends SQLStringQueryTest[AllCommerceDBs, (name: String, date: LocalDate)] {
  def testDescription = "Flow: project tuple, version of map that calls toRow for conversion"
  def query() =
    testDB.tables.buyers.map: b =>
      (name = b.name, date = b.dateOfBirth)
  def expectedQueryPattern: String = "SELECT buyers$A.name as name, buyers$A.dateOfBirth as date FROM buyers as buyers$A"
}