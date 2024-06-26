package test.query.flow
import test.{SQLStringQueryTest, SQLStringAggregationTest}
import test.query.{AllCommerceDBs, ShippingInfo, commerceDBs}
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

  def sqlString = "SELECT r0.name, r0.id FROM buyers r0"
}

class FlowForTest2 extends SQLStringQueryTest[AllCommerceDBs, (name: String, shippingDate: LocalDate)] {
  def testDescription = "Flow: project tuple, 2 nest, for comprehension"
  def query() =
    for
      b <- testDB.tables.buyers
      si <- testDB.tables.shipInfos
    yield (name = b.name, shippingDate = si.shippingDate).toRow

  def sqlString = "SELECT r0.name, r1.shippingDate FROM buyers r0, shipInfos r1"
}

class FlowForTest3 extends SQLStringQueryTest[AllCommerceDBs, String] {
  def testDescription = "Flow: single field, 1 nest, for comprehension"
  def query() =
    for
      b <- testDB.tables.buyers
    yield b.name

  def sqlString = "SELECT name FROM buyers"
}

class FlowForTest4 extends SQLStringQueryTest[AllCommerceDBs, String] {
  def testDescription = "Flow: single field, 2 nest, for comprehension"
  def query() =
    for
      b <- testDB.tables.buyers
      si <- testDB.tables.shipInfos
    yield b.name

  def sqlString = "SELECT b.name FROM buyers b, shipInfos si"
}

class FlowForIfTest5 extends SQLStringQueryTest[AllCommerceDBs, (bName: String, bId: Int)] {
  def testDescription = "Flow: project tuple, 1 nest, for comprehension + if"

  def query() =
    for
      b <- testDB.tables.buyers
      if b.id > 1
    yield (bName = b.name, bId = b.id).toRow

  def sqlString = "SELECT r0.name, r0.id FROM buyers r0 WHERE r0.id > 0"
}

class FlowForAggregateTest extends SQLStringQueryTest[AllCommerceDBs, (pName: String, sumP: Double)] {
  def testDescription = "Flow: for comprehension with agg"

  def query() =
    for
      p <- testDB.tables.products
    yield (pName = p.name, sumP = sum(p.price))

  def sqlString = "SELECT r0.name, r0.id FROM buyers r0"
}

class FlowMapTest1 extends SQLStringQueryTest[AllCommerceDBs, (bName: String, bId: Int)] {
  def testDescription = "Flow: project tuple, 1 nest, map"
  def query() =
    testDB.tables.buyers.map(b =>
      (bName = b.name, bId = b.id).toRow
    )

  def sqlString = "SELECT r0.name, r0.id FROM buyers r0"
}

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

class FlowMapFilterWithTest extends SQLStringQueryTest[AllCommerceDBs, (bName: String, bId: Int)] {
  def testDescription = "Flow: project tuple, 1 nest, map + filterWith"

  def query() =
    testDB.tables.buyers.withFilter(_.id > 1).map(b =>
      (bName = b.name, bId = b.id).toRow
    )
  def sqlString = "SELECT r0.name, r0.id FROM buyers r0 WHERE r0.id > 0"
}
class FlowMapFilterTest extends SQLStringQueryTest[AllCommerceDBs, (bName: String, bId: Int)] {
  def testDescription = "Flow: project tuple, 1 nest, map + filter"

  def query() =
    testDB.tables.buyers.filter(_.id > 1).map(b =>
      (bName = b.name, bId = b.id).toRow
    )
  def sqlString = "SELECT r0.name, r0.id FROM buyers r0 WHERE r0.id > 0"
}
class FlowMapFilterTest2 extends SQLStringQueryTest[AllCommerceDBs, (bName: String, bId: Int)] {
  def testDescription = "Flow: project tuple, 1 nest, filter after map"

  def query() =
    testDB.tables.buyers.map(b =>
      (bName = b.name, bId = b.id).toRow
    ).filter(_.bId > 1) // optimizer should be able to push predicate before map
  def sqlString = "SELECT r0.name, r0.id FROM buyers r0 WHERE r0.id > 0"
}

class FlowMapAggregateTest extends SQLStringQueryTest[AllCommerceDBs, Int] {
  def testDescription = "Flow: map on aggregate"

  def query() =
      testDB.tables.shipInfos.map(si =>
        sum(si.buyerId)
      )

  def sqlString = ""
}

class FlowMapAggregateTest2 extends SQLStringAggregationTest[AllCommerceDBs, Int] {
  def testDescription = "Flow: flatMap on aggregate"

  def query() =
    testDB.tables.shipInfos.flatMap(si =>
      sum(si.buyerId)
    )

  def sqlString = ""
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

class FlowMapAggregateTest6 extends SQLStringQueryTest[AllCommerceDBs, (sum: Int)] {
  def testDescription = "Flow: project + map on aggregate"

  def query() =
    testDB.tables.shipInfos.map(si =>
      (sum = sum(si.buyerId)).toRow
    )

  def sqlString = ""
}

class FlowMapAggregateTest7 extends SQLStringAggregationTest[AllCommerceDBs, (sum: Int)] {
  def testDescription = "Flow: project + flatMap on aggregate"

  def query() =
    testDB.tables.shipInfos.flatMap(si =>
      (sum = sum(si.buyerId)).toRow
    )

  def sqlString = ""
}

class FlowMapConvertedTest extends SQLStringQueryTest[AllCommerceDBs, (name: String, date: LocalDate)] {
  def testDescription = "Flow: project tuple, version of map that calls toRow for conversion"
  def query() =
    testDB.tables.buyers.map: b =>
      (name = b.name, date = b.dateOfBirth)
  def sqlString: String = "SELECT city.name AS name, city.zipcode AS zipcode FROM cities AS city"
}

class FlowMapAggregateConvertedTest extends SQLStringQueryTest[AllCommerceDBs, (sum: Int)] {
  def testDescription = "Flow: project tuple, version of map that calls toRow for conversion for aggregate"

  def query() =
    testDB.tables.shipInfos.map(si =>
      (sum = sum(si.buyerId))
    )

  def sqlString = ""
}

