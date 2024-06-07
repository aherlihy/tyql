package test.query.project
import test.SQLStringQueryTest
import test.query.{commerceDBs,  AllCommerceDBs, Product}

import tyql.*
import tyql.Expr.toRow
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

import java.time.LocalDate

class ReturnIntTest extends SQLStringQueryTest[AllCommerceDBs, Int] {
  def testDescription = "Project: return int"
  def query() =
    testDB.tables.products.map: c =>
      c.id

  def sqlString = """
  SELECT products.id
  FROM products
      """
}

class ReturnStringTest extends SQLStringQueryTest[AllCommerceDBs, String] {
  def testDescription = "Project: return string"
  def query() =
    testDB.tables.products.map: c =>
      c.name

  def sqlString = """SELECT products.name
  FROM products
      """
}

class ProjectIntTest extends SQLStringQueryTest[AllCommerceDBs, (id: Int)] {
  def testDescription = "Project: project int"
  def query() =
    testDB.tables.products.map: c =>
      (id = c.id).toRow

  def sqlString = """SELECT products.id
  FROM products
      """
}

class ProjectStringTest extends SQLStringQueryTest[AllCommerceDBs, (name: String)] {
  def testDescription = "Project: project string"
  def query() =
    testDB.tables.products.map: c =>
      (name = c.name).toRow

  def sqlString = """
  SELECT products.id
  FROM products
      """
}

class ProjectMixedTest extends SQLStringQueryTest[AllCommerceDBs, (name: String, id: Int)] {
  def testDescription = "Project: project string+int"
  def query() =
    testDB.tables.products.map: c =>
      (name = c.name, id = c.id).toRow

  def sqlString = """
  SELECT products.name, products.id
FROM products
      """
}

class ProjectString2Test extends SQLStringQueryTest[AllCommerceDBs, (name: String, name2: String)] {
  def testDescription = "Project: project string+string"
  def query() =
    testDB.tables.products.map: c =>
      (name = c.name, name2 = c.name).toRow

  def sqlString = """SELECT products.name, products.name AS name2
FROM products
      """
}

class JoinProjectInt2Test extends SQLStringQueryTest[AllCommerceDBs, (id: Int, id2: Int)] {
  def testDescription = "Project: project int+int"
  def query() =
    testDB.tables.products.map: c =>
      (id = c.id, id2 = c.id).toRow

  def sqlString = """SELECT products.id, products.id AS id2
FROM products
      """
}

class ProjectTest extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription = "Project: project entire row"
  def query() =
    testDB.tables.products.map: c =>
      c
  def sqlString = """SELECT products.*
FROM products
      """
}

class Project2Test extends SQLStringQueryTest[AllCommerceDBs, (id: Int, name: String, price: Double)] {
  def testDescription = "Project: project to tuple, toRow"
  def query() =
    testDB.tables.products.map: c =>
      (id = c.id, name = c.name, price =  c.price).toRow
  def sqlString ="""
  SELECT products.id, products.name, products.price
FROM products
        """
}

class Project3Test extends SQLStringQueryTest[AllCommerceDBs, (id: Int, name: String, price: Double, extra: Int)] {
  def testDescription = "Project: project to tuple with concat with literal"
  def query() =
    testDB.tables.products.map: c =>
      (id = c.id, name = c.name, price =  c.price).toRow.concat((extra = Expr.IntLit(1)))
  def sqlString ="""
  SELECT products.id, products.name, products.price, 1 AS extra
FROM products
        """
}

class Project4Test extends SQLStringQueryTest[AllCommerceDBs, (id: Int, name: String, price: Double, buyerId: Int, shippingDate: LocalDate)] {
  def testDescription = "Project: project to tuple with concat with another tuple"
  def query() =
    val tupleProd = testDB.tables.products
      .map: c =>
        (id = c.id, name = c.name, price = c.price).toRow
    val tupleShip = testDB.tables.shipInfos
      .map: s =>
        (buyerId = s.buyerId, shippingDate = s.shippingDate).toRow
    for
      t1 <- tupleProd
      t2 <- tupleShip
    yield t1.concat(t2)

  //    tupleProd.flatMap: c =>
  //      tupleShip.map: s =>
  //        s.concat(c)

  def sqlString ="""SELECT p.id, p.name, p.price, s.buyerId, s.shippingDate
FROM products p
CROSS JOIN shipInfos s
        """
}

/** TODO:
 * Concat doesn't work on these tests because the original types are defined as case classes.
 * Should we have some automatic conversion from case class to named tuple, or generate a named
 * tuple from a case class + extra fields? Otherwise it's odd that you can't call concat on a
 * normal row.
 */

//class ProjectConcatTest extends SQLStringQueryTest[AllCommerceDBs, (id: Int, name: String, price: Double, extra: Int)] {
//  def testDescription = "Project: project + concat literal"
//  def query() =
//    testDB.tables.products.map: c =>
//      c.concat((extra = 1))
//  def sqlString = """
//      """
//}

//class ProjectJoinConcatTest extends SQLStringQueryTest[AllCommerceDBs, (id: Int, name: String, price: Double, buyerId: Int, shippingDate: LocalDate)] {
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