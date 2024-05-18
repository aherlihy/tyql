package test.query.project
import test.SQLStringTest
import test.query.{commerceDBs,  AllCommerceDBs, Product}

import tyql.*
import tyql.Expr.toRow
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

import java.time.LocalDate

class ReturnIntTest extends SQLStringTest[AllCommerceDBs, Int] {
  def testDescription = "Project: return int"
  def query() =
    testDB.tables.products.map: c =>
      c.id

  def sqlString = """
      """
}

class ReturnStringTest extends SQLStringTest[AllCommerceDBs, String] {
  def testDescription = "Project: return string"
  def query() =
    testDB.tables.products.map: c =>
      c.name

  def sqlString = """
      """
}

class ProjectIntTest extends SQLStringTest[AllCommerceDBs, (id: Int)] {
  def testDescription = "Project: project int"
  def query() =
    testDB.tables.products.map: c =>
      (id = c.id).toRow

  def sqlString = """
      """
}

class ProjectStringTest extends SQLStringTest[AllCommerceDBs, (name: String)] {
  def testDescription = "Project: project string"
  def query() =
    testDB.tables.products.map: c =>
      (name = c.name).toRow

  def sqlString = """
      """
}

class ProjectMixedTest extends SQLStringTest[AllCommerceDBs, (name: String, id: Int)] {
  def testDescription = "Project: project string+int"
  def query() =
    testDB.tables.products.map: c =>
      (name = c.name, id = c.id).toRow

  def sqlString = """
      """
}

class ProjectString2Test extends SQLStringTest[AllCommerceDBs, (name: String, name2: String)] {
  def testDescription = "Project: project string+string"
  def query() =
    testDB.tables.products.map: c =>
      (name = c.name, name2 = c.name).toRow

  def sqlString = """
      """
}

class JoinProjectInt2Test extends SQLStringTest[AllCommerceDBs, (id: Int, id2: Int)] {
  def testDescription = "Project: project int+int"
  def query() =
    testDB.tables.products.map: c =>
      (id = c.id, id2 = c.id).toRow

  def sqlString = """
      """
}

class ProjectTest extends SQLStringTest[AllCommerceDBs, Product] {
  def testDescription = "Project: project entire row"
  def query() =
    testDB.tables.products.map: c =>
      c
  def sqlString = """
      """
}

class Project2Test extends SQLStringTest[AllCommerceDBs, (id: Int, name: String, price: Double)] {
  def testDescription = "Project: project to tuple, toRow"
  def query() =
    testDB.tables.products.map: c =>
      (id = c.id, name = c.name, price =  c.price).toRow
  def sqlString ="""
        """
}

class Project3Test extends SQLStringTest[AllCommerceDBs, (id: Int, name: String, price: Double, extra: Int)] {
  def testDescription = "Project: project to tuple with concat with literal"
  def query() =
    testDB.tables.products.map: c =>
      (id = c.id, name = c.name, price =  c.price).concat((extra = Expr.IntLit(1)))
  def sqlString ="""
        """
}

//class Project4Test extends SQLStringTest[AllCommerceDBs, (id: Int, name: String, price: Double, buyerId: Int, shippingDate: LocalDate)] {
//  def testDescription = "Project: project to tuple with concat with another tuple"
//  def query() =
//    val tupleProd = testDB.tables.products
//      .map: c =>
//        (id = c.id, name = c.name, price = c.price).toRow
//    val tupleShip = testDB.tables.shipInfos
//      .map: s =>
//        (buyerId = s.buyerId, shippingDate = s.shippingDate).toRow
//    for
//      t1 <- tupleProd
//      t2 <- tupleShip
//    yield t1.concat(t2)
//
////    tupleProd.flatMap: c =>
////      tupleShip.map: s =>
////        s.concat(c)
//
//  def sqlString ="""
//        """
//}

/** TODO:
 * Concat doesn't work on these tests because the original types are defined as case classes.
 * Should we have some automatic conversion from case class to named tuple, or generate a named
 * tuple from a case class + extra fields? Otherwise it's odd that you can't call concat on a
 * normal row.
 */

//class ProjectConcatTest extends SQLStringTest[AllCommerceDBs, (id: Int, name: String, price: Double, extra: Int)] {
//  def testDescription = "Project: project + concat literal"
//  def query() =
//    testDB.tables.products.map: c =>
//      c.concat((extra = 1))
//  def sqlString = """
//      """
//}

//class ProjectJoinConcatTest extends SQLStringTest[AllCommerceDBs, (id: Int, name: String, price: Double, buyerId: Int, shippingDate: LocalDate)] {
//  def testDescription = "Project: project + concat literal"
//  def query() =
//    testDB.tables.products.flatMap(c =>
//      testDB.tables.shipInfos
////        .map(s => (buyerId = s.buyerId, shippingDate = s.shippingDate))
//        .map(s =>
//          s.toRow//.concat(c.toRow)
//        )
//    )
//  def sqlString = """
//      """
//}