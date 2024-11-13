package test.query.flow
import test.{SQLStringQueryTest, SQLStringAggregationTest}
import test.query.{AllCommerceDBs, ShippingInfo, commerceDBs, Buyer}
import tyql.*

import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions
import java.time.LocalDate
import tyql.Expr.{sum, max}

class FlowForTest1 extends SQLStringQueryTest[AllCommerceDBs, (bName: String, bId: Int)] {
  def testDescription = "Flow: project tuple, 1 nest, for comprehension"
  def query() =
    for
      b <- testDB.tables.buyers
    yield (bName = b.name, bId = b.id).toRow

  def expectedQueryPattern = "SELECT buyers$A.name as bName, buyers$A.id as bId FROM buyers as buyers$A"
}

// TODO: toRow
//class FlowForTest1a extends SQLStringQueryTest[AllCommerceDBs, (bName: String, bId: Int)] {
//  def testDescription = "Flow: project tuple, 1 nest, for comprehension, without toRow"
//
//  def query() =
//    for
//      b <- testDB.tables.buyers
//    yield (bName = b.name, bId = b.id)
//
//  def expectedQueryPattern = "SELECT buyers$A.name as bName, buyers$A.id as bId FROM buyers as buyers$A"
//}

class FlowForTest2 extends SQLStringQueryTest[AllCommerceDBs, (name: String, shippingDate: LocalDate)] {
  def testDescription = "Flow: project tuple, 2 nest, for comprehension"
  def query() =
    for
      b <- testDB.tables.buyers
      si <- testDB.tables.shipInfos
    yield (name = b.name, shippingDate = si.shippingDate).toRow

  def expectedQueryPattern =
    "SELECT buyers$A.name as name, shippingInfo$B.shippingDate as shippingDate FROM buyers as buyers$A, shippingInfo as shippingInfo$B"
}

class FlowForTest3 extends SQLStringQueryTest[AllCommerceDBs, String] {
  def testDescription = "Flow: single field, 1 nest, for comprehension"
  def query() =
    for
      b <- testDB.tables.buyers
    yield b.name

  def expectedQueryPattern = "SELECT buyers$A.name FROM buyers as buyers$A"
}

class FlowForTest4 extends SQLStringQueryTest[AllCommerceDBs, String] {
  def testDescription = "Flow: single field, 2 nest, for comprehension"
  def query() =
    for
      b <- testDB.tables.buyers
      si <- testDB.tables.shipInfos
    yield b.name

  def expectedQueryPattern = "SELECT buyers$A.name FROM buyers as buyers$A, shippingInfo as shippingInfo$B"
}

class FlowForTest4Test extends SQLStringQueryTest[AllCommerceDBs, (name: String, id: Int, sd: LocalDate)] {
  def testDescription = "Flow: 3 nest, for comprehension"

  def query() =
    for
      purch <- testDB.tables.purchases
      prod <- testDB.tables.products
      si <- testDB.tables.shipInfos
      if purch.id == prod.id && prod.id == si.id
    yield (name = prod.name, id = purch.id, sd = si.shippingDate).toRow

  def expectedQueryPattern =
    """
        SELECT product$A.name as name, purchase$B.id as id, shippingInfo$C.shippingDate as sd
        FROM
          purchase as purchase$B,
          product as product$A,
          shippingInfo as shippingInfo$C
        WHERE purchase$B.id = product$A.id AND product$A.id = shippingInfo$C.id
      """
}

class FlowForIfTest5 extends SQLStringQueryTest[AllCommerceDBs, (bName: String, bId: Int)] {
  def testDescription = "Flow: project tuple, 1 nest, for comprehension + if"

  def query() =
    for
      b <- testDB.tables.buyers
      if b.id > 1
    yield (bName = b.name, bId = b.id).toRow

  def expectedQueryPattern =
    "SELECT buyers$A.name as bName, buyers$A.id as bId FROM buyers as buyers$A WHERE buyers$A.id > 1"
}

class FlowFlatmapMapTest1 extends SQLStringQueryTest[AllCommerceDBs, (bName: String, pId: Int)] {
  def testDescription = "Flow: project tuple, 1 nest, map"

  def query() =
    testDB.tables.products.flatMap(p =>
      testDB.tables.buyers.map(b =>
        (bName = b.name, pId = p.id).toRow
      )
    )

  def expectedQueryPattern =
    "SELECT buyers$1.name as bName, product$0.id as pId FROM product as product$0, buyers as buyers$1"
}
class FlowMapTest1 extends SQLStringQueryTest[AllCommerceDBs, (bName: String, bId: Int)] {
  def testDescription = "Flow: project tuple, 1 nest, map"
  def query() =
    testDB.tables.buyers.map(b =>
      (bName = b.name, bId = b.id).toRow
    )

  def expectedQueryPattern = "SELECT buyers$A.name as bName, buyers$A.id as bId FROM buyers as buyers$A"
}
// TODO: toRow
//class FlowMapTest1b extends SQLStringQueryTest[AllCommerceDBs, (bName: String, bId: Int)] {
//  def testDescription = "Flow: project tuple, 1 nest, map no toRow"
//  def query() =
//    testDB.tables.buyers.map(b =>
//      (bName = b.name, bId = b.id)
//    )
//
//  def expectedQueryPattern = "SELECT buyers$A.name as bName, buyers$A.id as bId FROM buyers as buyers$A"
//}
class FlowAggTest1b extends SQLStringAggregationTest[AllCommerceDBs, (bIdSum: Int, bIdMax: Int)] {
  def testDescription = "Flow: aggregate with aggregate, toRow"
  def query() =
    testDB.tables.buyers.aggregate(b =>
      (bIdSum = sum(b.id), bIdMax = max(b.id)).toRow
    )

  def expectedQueryPattern = "SELECT SUM(buyers$0.id) as bIdSum, MAX(buyers$0.id) as bIdMax FROM buyers as buyers$0"
}

class FlowFlatmapAggTest1b extends SQLStringAggregationTest[AllCommerceDBs, (bIdSum: Int, pIdMax: Int)] {
  def testDescription = "Flow: aggregate with aggregate nested in flatMap"
  def query() =
    testDB.tables.products.aggregate(p =>
      testDB.tables.buyers.aggregate(b =>
        (bIdSum = sum(b.id), pIdMax = max(p.id)).toRow
      )
    )

  def expectedQueryPattern =
    "SELECT SUM(buyers$7.id) as bIdSum, MAX(product$6.id) as pIdMax FROM product as product$6, buyers as buyers$7"
}

class FlowFlatMapTest2 extends SQLStringQueryTest[AllCommerceDBs, (name: String, shippingDate: LocalDate)] {
  def testDescription = "Flow: project tuple, 2 nest, flatmap+map"

  def query() =
    testDB.tables.buyers.flatMap(b =>
      testDB.tables.shipInfos.map(si =>
        (name = b.name, shippingDate = si.shippingDate).toRow
      )
    )

  def expectedQueryPattern =
    "SELECT buyers$A.name as name, shippingInfo$B.shippingDate as shippingDate FROM buyers as buyers$A, shippingInfo as shippingInfo$B"
}

class FlowFlatMapTest3
    extends SQLStringQueryTest[AllCommerceDBs, (name: String, shippingDateA: LocalDate, shippingDateB: LocalDate)] {
  def testDescription = "Flow: project tuple, 3 nest, flatMap + flatmap+map"

  def query() =
    testDB.tables.shipInfos.flatMap(si1 =>
      testDB.tables.buyers.flatMap(b =>
        testDB.tables.shipInfos.map(si2 =>
          (name = b.name, shippingDateA = si1.shippingDate, shippingDateB = si2.shippingDate).toRow
        )
      )
    )

  def expectedQueryPattern = """
     SELECT
        buyers$A.name as name,
        shippingInfo$B.shippingDate as shippingDateA,
        shippingInfo$C.shippingDate as shippingDateB
     FROM
        shippingInfo as shippingInfo$B,
        buyers as buyers$A,
        shippingInfo as shippingInfo$C
     """
}

class FlowMapFilterWithTest extends SQLStringQueryTest[AllCommerceDBs, (bName: String, bId: Int)] {
  def testDescription = "Flow: project tuple, 1 nest, map + filterWith"

  def query() =
    testDB.tables.buyers.withFilter(_.id > 1).map(b =>
      (bName = b.name, bId = b.id).toRow
    )
  def expectedQueryPattern =
    "SELECT buyers$A.name as bName, buyers$A.id as bId FROM buyers as buyers$A WHERE buyers$A.id > 1"
}

class FlowMapFilterTest extends SQLStringQueryTest[AllCommerceDBs, (bName: String, bId: Int)] {
  def testDescription = "Flow: project tuple, 1 nest, map + filter"

  def query() =
    testDB.tables.buyers.filter(_.id > 1).map(b =>
      (bName = b.name, bId = b.id).toRow
    )
  def expectedQueryPattern =
    "SELECT buyers$A.name as bName, buyers$A.id as bId FROM buyers as buyers$A WHERE buyers$A.id > 1"
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

  def expectedQueryPattern =
    "SELECT buyers$A.name as bName, buyers$A.id as bId FROM buyers as buyers$A WHERE buyers$A.id > 100 AND buyers$A.id > 10 AND buyers$A.id > 1"
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
  def expectedQueryPattern = """
    SELECT
      subquery$A.bName2 as bName3, subquery$A.bId2 as bId3
    FROM
      (SELECT
          subquery$B.bName as bName2, subquery$B.bId as bId2
       FROM
         (SELECT
            buyers$C.name as bName, buyers$C.id as bId
          FROM buyers as buyers$C) as subquery$B) as subquery$A
    """
}

class FlowAllSubsequentFilterTest extends SQLStringQueryTest[AllCommerceDBs, Buyer] {
  def testDescription = "Flow: all, 1 nest, filter x 3"

  def query() =
    testDB.tables.buyers
      .filter(_.id > 1)
      .filter(_.id > 10) // ignore nonsensical constraints
      .filter(_.id > 100)

  def expectedQueryPattern =
    "SELECT * FROM buyers as buyers$A WHERE buyers$A.id > 100 AND buyers$A.id > 10 AND buyers$A.id > 1"
}

class FlowMapFilterTest2 extends SQLStringQueryTest[AllCommerceDBs, (bName: String, bId: Int)] {
  def testDescription = "Flow: project tuple, 1 nest, filter after map"

  def query() =
    testDB.tables.buyers.map(b =>
      (bName = b.name, bId = b.id).toRow
    ).filter(_.bId > 1) // for now generate subquery
  def expectedQueryPattern =
    "SELECT * FROM (SELECT buyers$A.name as bName, buyers$A.id as bId FROM buyers as buyers$A) as subquery$B WHERE subquery$B.bId > 1"
}

class FlowMapAggregateTest extends SQLStringAggregationTest[AllCommerceDBs, Int] {
  def testDescription = "Flow: aggregate with single sum"

  def query() =
    testDB.tables.shipInfos.aggregate(si =>
      sum(si.buyerId)
    )

  def expectedQueryPattern = "SELECT SUM(shippingInfo$A.buyerId) FROM shippingInfo as shippingInfo$A"
}

class FlowMapAggregateTest7 extends SQLStringAggregationTest[AllCommerceDBs, (sum: Int)] {
  def testDescription = "Flow: project + aggregate"

  def query() =
    testDB.tables.shipInfos.aggregate(si =>
      (sum = sum(si.buyerId)).toRow
    )

  def expectedQueryPattern = "SELECT SUM(shippingInfo$30.buyerId) as sum FROM shippingInfo as shippingInfo$30"
}

// TODO: toRow
//class FlowMapAggregateConvertedTest extends SQLStringQueryTest[AllCommerceDBs, (sum: Int)] {
//  def testDescription = "Flow: project tuple, version of map that calls toRow for conversion for aggregate"
//
//  def query() =
//    testDB.tables.shipInfos.map(si =>
//      (sum = sum(si.buyerId))
//    )
//
//  def expectedQueryPattern = "SELECT SUM(shippingInfo$A.buyerId) as sum FROM shippingInfo as shippingInfo$A"
//}
//
//class FlowMapConvertedTest extends SQLStringQueryTest[AllCommerceDBs, (name: String, date: LocalDate)] {
//  def testDescription = "Flow: project tuple, version of map that calls toRow for conversion"
//  def query() =
//    testDB.tables.buyers.map: b =>
//      (name = b.name, date = b.dateOfBirth)
//  def expectedQueryPattern: String = "SELECT buyers$A.name as name, buyers$A.dateOfBirth as date FROM buyers as buyers$A"
//}
