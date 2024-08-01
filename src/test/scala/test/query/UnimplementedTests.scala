//package test.query.unimplemented
//
//import test.SQLStringQueryTest
//import test.query.{AllCommerceDBs, Buyer, commerceDBs, Product, Purchase}
//import tyql.*
//import tyql.Expr.{max, min, toRow}
//
//import language.experimental.namedTuples
//import NamedTuple.*

//// TODO: sort relation ops so that subsequent ORDER BY/etc get collapsed
//class UnimplementedSortLimitSortLimitSubqueryTest extends SQLStringQueryTest[AllCommerceDBs, String] {
//  def testDescription = "Subquery: sortLimitSortLimit"
//  def query() =
//    testDB.tables.products.sort(_.price, Ord.DESC).take(4).sort(_.price, Ord.ASC).take(2).map(_.name)
//  def expectedQueryPattern = """
//        SELECT
//          subquery$C.name
//        FROM
//            (SELECT * FROM products as products$A ORDER BY products$A.price DESC LIMIT 4) as subquery$D
//        ORDER BY subquery$D.price ASC LIMIT 2)
//      """
//}
//
//// TODO: not sure if this should be allowed, or what the generated query should look like
//class UnimplmentedNestedJoinExtra3Test extends SQLStringQueryTest[AllCommerceDBs, (name: String, newId: Int)] {
//  def testDescription = "Subquery: nested flatMaps with map from both"
//
//  def query() =
//    testDB.tables.purchases.flatMap(purch =>
//      testDB.tables.products.map(prod =>
//        (name = prod.name, newId = purch.id).toRow
//      ).filter(s => s.newId == 10)
//    )
//
//
//  def expectedQueryPattern =
//    """
//      SELECT
//            product$A.name as name,
//            purchase$B.id as id
//        FROM
//          purchase as purchase$B,
//          product as product$A
//      WHERE (purchase$B.id = 1 AND product$A.name = "test")
//    """
//}
