package test.query.repro

import test.SQLStringQueryTest
import tyql.{Expr, NExpr, Query, RestrictedQuery}

import language.experimental.namedTuples
import test.query.{AllCommerceDBs, commerceDBs}
import test.query.recursive.{Location, CSPADB, CSPADBs}
import tyql.Query.fix

class ToRowTest extends SQLStringQueryTest[AllCommerceDBs, (bName: String, bId: Int)] {
  def testDescription = "The definition of map that takes in a tuple-of-expr does not work"

  def query() =
    testDB.tables.buyers.map(b =>
      (bName = b.name, bId = b.id).toRow
//      (bName = b.name, bId = b.id) // this is what I want to work
    )

  def expectedQueryPattern = "SELECT buyers$A.name as bName, buyers$A.id as bId FROM buyers as buyers$A"
}

class IntermediateValTest extends SQLStringQueryTest[AllCommerceDBs, (innerK1: Int, innerK2: String)] {
  def testDescription = "Assigning to an intermediate value means compiler can't tell what the function returns, so doesn't know which flatMap to use"

  def query() =
    // I would like this to work without the type ascription on the line below, but it's needed to compile
    val lhs: Query[(outerK1: Int, outerK2: Int)] = testDB.tables.purchases.flatMap(p1 =>
      testDB.tables.products.map(p2 => (outerK1 = p1.id, outerK2 = p2.id).toRow)
    )

    lhs.flatMap(p3 =>
      testDB.tables.buyers.map(p4 => (innerK1 = p3.outerK1, innerK2 = p4.name).toRow)
    )


  def expectedQueryPattern =
    """
      SELECT
        subquery$A.outerK1 as innerK1, buyers$B.name as innerK2
      FROM
        (SELECT
          purchase$C.id as outerK1, product$D.id as outerK2
        FROM
          purchase as purchase$C,
          product as product$D) as subquery$A,
        buyers as buyers$B
      """
}
class RecursiveFlatmapOverloadingTest extends SQLStringQueryTest[CSPADB, Location] {
  def testDescription: String = "Nested flatMaps *without* intermediate value with type ascription also don't work"

  def query() =
    val assign = testDB.tables.assign
    val dereference = testDB.tables.dereference

    // this is needed for it to work
    dereference.fix(valueAlias =>
      val fn: Expr.Ref[Location, NExpr] => RestrictedQuery[Location] = d1 =>
        valueAlias.flatMap(va =>
          dereference
            .filter(d2 => d1.p1 == va.p1 && va.p2 == d2.p1)
            .map(d2 => (p1 = d1.p2, p2 = d2.p2).toRow)
        )
      dereference.flatMap(fn)
    )
    // this is what i want to work
//    dereference.fix(valueAlias =>
//      valueAlias.flatMap(va =>
//        dereference.flatMap(d1 =>
//          dereference
//            .filter(d2 => d1.p1 == va.p1 && va.p2 == d2.p1)
//            .map(d2 => (p1 = d1.p2, p2 = d2.p2).toRow)
//        )
//      )
//    )

  def expectedQueryPattern: String = "WITH RECURSIVE recursive$10 AS (SELECT * FROM dereference as dereference$10 UNION ALL SELECT dereference$12.p2 as p1, dereference$13.p2 as p2 FROM dereference as dereference$12, recursive$10 as ref$6, dereference as dereference$13 WHERE dereference$12.p1 = ref$6.p1 AND ref$6.p2 = dereference$13.p1) SELECT * FROM recursive$10 as recref$0"
}


