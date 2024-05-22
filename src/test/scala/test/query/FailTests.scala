package test.query

import test.SQLStringTest

import java.time.LocalDate
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