package test.query.relationops
import test.SQLStringQueryTest
import test.query.{commerceDBs,  AllCommerceDBs, Product}

import tyql.*
import tyql.Expr.toRow
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

class RelationOpsUnionTest extends SQLStringQueryTest[AllCommerceDBs, (id: Int)] {
  def testDescription: String = "RelationOps: union with project"
  def query() =
    testDB.tables.products
      .map: prod =>
        (id = prod.id).toRow
      .union(testDB.tables.purchases
        .map: purch =>
          (id = purch.id).toRow
      )
  def expectedQueryPattern: String =  """
        SELECT product$A.id as id
        FROM product as product$A
        UNION
        SELECT purchase$B.id as id
        FROM purchase as purchase$B
      """
}

class RelationOpsUnion2Test extends SQLStringQueryTest[AllCommerceDBs, Product] {
  def testDescription: String = "RelationOps: union"
  def query() =
    testDB.tables.products
      .map(prod => prod)
      .union(testDB.tables.products.map(purch => purch))
  def expectedQueryPattern: String =  """
        SELECT *
        FROM product as product$A
        UNION
        SELECT *
        FROM product as product$B
      """
}

class RelationOpsUnionAllTest extends SQLStringQueryTest[AllCommerceDBs, (id: Int)] {
  def testDescription: String = "RelationOps: unionAll with project"
  def query() =
    testDB.tables.products
      .map: prod =>
        (id = prod.id).toRow
      .unionAll(testDB.tables.purchases
        .map: purch =>
          (id = purch.id).toRow
      )
  def expectedQueryPattern: String =  """
        SELECT product$A.id as id
        FROM product as product$A
        UNION ALL
        SELECT purchase$B.id as id
        FROM purchase as purchase$B
      """
}

class RelationOpsIntersectTest extends SQLStringQueryTest[AllCommerceDBs, (id: Int)] {
  def testDescription: String = "RelationOps: intersect with project"
  def query() =
    testDB.tables.products
      .map: prod =>
        (id = prod.id).toRow
      .intersect(testDB.tables.purchases
        .map: purch =>
          (id = purch.id).toRow
      )
  def expectedQueryPattern: String =  """
        SELECT product$A.id as id
        FROM product as product$A
        INTERSECT
        SELECT purchase$B.id as id
        FROM purchase as purchase$B
      """
}

