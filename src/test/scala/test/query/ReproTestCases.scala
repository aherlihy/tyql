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

object Repro {

}