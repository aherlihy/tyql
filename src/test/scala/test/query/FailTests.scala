//package test.query.fail
//
//import test.{SQLStringAggregationTest, SQLStringQueryTest}
//import tyql.Expr.sum
//
//import java.time.LocalDate
// TODO: tests that should fail to compile

//class FlowMapTest extends SQLStringTest[AllCommerceDBs, (name: String, shippingDate: LocalDate)] {
//  def testDescription = "Flow: project tuple, 2 nest, map+map should fail with useful error message"
//  def query() =
//    testDB.tables.buyers.map(b =>
//      testDB.tables.shipInfos.map(si =>
//        (name = b.name, shippingDate = si.shippingDate).toRow
//      )
//    )
//
//  def sqlString = "SELECT r0.name, r1.shippingDate FROM buyers r0, shipInfos r1"
//}
//
//class FlowMapAggregate3Test extends SQLStringTest[AllCommerceDBs, Int] {
//  def testDescription = "Flow: project tuple, 2 nest, map+map should fail even with aggregation"
//  def query() =
//    testDB.tables.buyers.map(b =>
//      testDB.tables.shipInfos.map(si =>
//        si.buyerId.sum
//      )
//    )
//
//  def sqlString = "SELECT MAX(r1.buyerId) FROM buyers r0, shipInfos r1"
//}

//class AggregationSum3SelectTest extends SQLStringTest[AllCommerceDBs, Double] {
//  def testDescription: String = "Aggregation: sum with map"
//  def query() =
//    testDB.tables.products.withFilter(p => p.price != 0).sum(_.price).map(_) // should fail to compile because cannot call query methods on aggregation result
//  def sqlString: String = "SELECT SUM(product.price) FROM product WHERE product.price > 0"
//}

//class OuterFlatMapTest extends SQLStringTest[AllCommerceDBs, (bName: String, bId: Int)] {
//  def testDescription = "Flow: project tuple, no nest, flatMap"
//
//  def query() =
//    testDB.tables.buyers.flatMap(b =>
//      (bName = b.name, bId = b.id).toRow
//    )
//    ???
//  def sqlString = "SELECT r0.name, r0.id FROM buyers r0 WHERE r0.id > 0"
//}
//class OuterFlatMapNestedFlatMapTest extends SQLStringTest[AllCommerceDBs, (name: String, shippingDate: LocalDate)] {
//  def testDescription = "Flow: project tuple, 2 nest, flatmap+flatmap"
//
//  def query() =
//    testDB.tables.buyers.flatMap(b =>
//      testDB.tables.shipInfos.flatMap(si =>
//        (name = b.name, shippingDate = si.shippingDate).toRow
//      )
//    )
//    ???
//
//  def sqlString = "SELECT r0.name, r1.shippingDate FROM buyers r0, shipInfos r1"
//}
//
//class OuterMapNestedMapTest extends SQLStringTest[AllCommerceDBs, (name: String, shippingDate: LocalDate)] {
//  def testDescription = "Flow: project tuple, 2 nest, map+map"
//
//  def query() =
//    testDB.tables.buyers.map(b =>
//      testDB.tables.shipInfos.map(si =>
//        (name = b.name, shippingDate = si.shippingDate).toRow
//      )
//    )
//    ???
//
//  def sqlString = "SELECT r0.name, r1.shippingDate FROM buyers r0, shipInfos r1"
//}
//class OuterMapNestedFlatMapTest extends SQLStringTest[AllCommerceDBs, (name: String, shippingDate: LocalDate)] {
//  def testDescription = "Flow: project tuple, 2 nest, map+flatmap"
//
//  def query() =
//    testDB.tables.buyers.map(b =>
//      testDB.tables.shipInfos.flatMap(si =>
//        (name = b.name, shippingDate = si.shippingDate).toRow
//      )
//    )
//    ???
//
//  def sqlString = "SELECT r0.name, r1.shippingDate FROM buyers r0, shipInfos r1"
//}

//class AggregateScalarFailTest extends SQLStringAggregationTest[AllCommerceDBs, Double] {
//  def testDescription: String = "Aggregation: aggregate + scalar expr should fail"
//
//  def query() =
//    testDB.tables.products
//      .aggregate(p => p.price)
//
//  def sqlString: String = "SELECT SUM(purchase.price) FROM purchase"
//}
//class AggregationSubqueryFailTest extends SQLStringQueryTest[AllCommerceDBs, Boolean] {
//  def testDescription: String = "Aggregation: aggregate with aggregate subquery as source (should fail)"
//
//  def query() =
//    testDB.tables.products.aggregate(p => (avgPrice = p.price.avg)).map(r => r == 10)
//
//  def sqlString: String =
//    """
//          SELECT avgPrice = 10
//          FROM (SELECT AVG(price) as avgPrice FROM products)
//        """
//}
//class FlowMapAggregateTestFail extends SQLStringAggregationTest[AllCommerceDBs, Int] {
//  def testDescription = "Flow: 2 nest, map+map should fail even with aggregate"
//
//  def query() =
//    testDB.tables.buyers.map(b =>
//      testDB.tables.shipInfos.map(si =>
//        sum(si.buyerId)
//      )
//    )
//
//  def sqlString = ""
//}
