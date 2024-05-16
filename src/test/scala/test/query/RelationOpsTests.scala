package test.query.relationops
import test.SQLStringTest
import test.query.{commerceDBs,  AllCommerceDBs, Product}

import tyql.*
import tyql.Expr.toRow
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

class RelationOpsUnionTest extends SQLStringTest[AllCommerceDBs, (id: Int)] {
  def testDescription: String = "RelationOps: union with project"
  def query() =
    testDB.tables.products
      .map: prod =>
        (id = prod.id).toRow
      .union(testDB.tables.purchases
        .map: purch =>
          (id = purch.id).toRow
      )
  def sqlString = """
        SELECT product.id
        FROM product
        UNION
        SELECT purchase.id
        FROM purchases
      """
}

class RelationOpsUnion2Test extends SQLStringTest[AllCommerceDBs, Product] {
  def testDescription: String = "RelationOps: union"
  def query() =
    testDB.tables.products
      .map(prod => prod)
      .union(testDB.tables.products.map(purch => purch))
  def sqlString = """
        SELECT *
        FROM product
        UNION
        SELECT *
        FROM product
      """
}

class RelationOpsUnionAllTest extends SQLStringTest[AllCommerceDBs, (id: Int)] {
  def testDescription: String = "RelationOps: unionAll with project"
  def query() =
    testDB.tables.products
      .map: prod =>
        (id = prod.id).toRow
      .unionAll(testDB.tables.purchases
        .map: purch =>
          (id = purch.id).toRow
      )
  def sqlString = """
        SELECT product.id
        FROM product
        UNION ALL
        SELECT purchase.id
        FROM purchases
      """
}

class RelationOpsIntersectTest extends SQLStringTest[AllCommerceDBs, (id: Int)] {
  def testDescription: String = "RelationOps: intersect with project"
  def query() =
    testDB.tables.products
      .map: prod =>
        (id = prod.id).toRow
      .intersect(testDB.tables.purchases
        .map: purch =>
          (id = purch.id).toRow
      )
  def sqlString = """
        SELECT product.id
        FROM product
        UNION
        SELECT purchase.id
        FROM purchases
      """
}

