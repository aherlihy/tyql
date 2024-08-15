package test.query.fail

import test.SQLStringAggregationTest
import tyql.*
import test.query.{commerceDBs, AllCommerceDBs}
import language.experimental.namedTuples

class MapMapCompileErrorTest extends munit.FunSuite {
  def testDescription: String = "map+map should fail with useful error message"
  def expectedError: String = "Cannot return a Query from a map. Did you mean to use flatMap?"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.Table
           import java.time.LocalDate

           case class Product(id: Int, name: String, price: Double)

           case class Buyer(id: Int, name: String, dateOfBirth: LocalDate)

           case class ShippingInfo(id: Int, buyerId: Int, shippingDate: LocalDate)

           case class Purchase(
                     id: Int,
                     shippingInfoId: Int,
                     productId: Int,
                     count: Int,
                     total: Double
                   )

           val tables = (
             products = Table[Product]("product"),
             buyers = Table[Buyer]("buyers"),
             shipInfos = Table[ShippingInfo]("shippingInfo"),
             purchases = Table[Purchase]("purchase")
           )

           // TEST
           tables.buyers.map(b =>
             tables.shipInfos.map(si =>
               (name = b.name, shippingDate = si.shippingDate)
             )
           )
        """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class MapMapTRCompileErrorTest extends munit.FunSuite {
  def testDescription: String = "map+map with toRow should fail with useful error message"
  def expectedError: String = "Cannot return a Query from a map. Did you mean to use flatMap?"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.Table
           import java.time.LocalDate

           case class Product(id: Int, name: String, price: Double)

           case class Buyer(id: Int, name: String, dateOfBirth: LocalDate)

           case class ShippingInfo(id: Int, buyerId: Int, shippingDate: LocalDate)

           case class Purchase(
                     id: Int,
                     shippingInfoId: Int,
                     productId: Int,
                     count: Int,
                     total: Double
                   )

           val tables = (
             products = Table[Product]("product"),
             buyers = Table[Buyer]("buyers"),
             shipInfos = Table[ShippingInfo]("shippingInfo"),
             purchases = Table[Purchase]("purchase")
           )

          // TEST
          tables.buyers.map(b =>
            tables.shipInfos.map(si =>
              (name = b.name, shippingDate = si.shippingDate).toRow
            )
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class MapMapAggregateCompileErrorTest extends munit.FunSuite {
  def testDescription: String = "map+map with aggregate should fail with useful error message"
  def expectedError: String = "Cannot return a Query from a map. Did you mean to use flatMap?"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.{Table, Expr}
           import java.time.LocalDate

           case class Product(id: Int, name: String, price: Double)

           case class Buyer(id: Int, name: String, dateOfBirth: LocalDate)

           case class ShippingInfo(id: Int, buyerId: Int, shippingDate: LocalDate)

           case class Purchase(
                     id: Int,
                     shippingInfoId: Int,
                     productId: Int,
                     count: Int,
                     total: Double
                   )

           val tables = (
             products = Table[Product]("product"),
             buyers = Table[Buyer]("buyers"),
             shipInfos = Table[ShippingInfo]("shippingInfo"),
             purchases = Table[Purchase]("purchase")
           )

          // TEST
          tables.buyers.map(b =>
            tables.shipInfos.map(si =>
              Expr.sum(si.buyerId)
            )
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class MapAfterAggregateCompileErrorTest extends munit.FunSuite {
  def testDescription: String = "map after aggregate should fail with useful error message"
  def expectedError: String = "value map is not a member of tyql.Aggregation"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.{Table, Expr}
           import java.time.LocalDate

           case class Product(id: Int, name: String, price: Double)

           case class Buyer(id: Int, name: String, dateOfBirth: LocalDate)

           case class ShippingInfo(id: Int, buyerId: Int, shippingDate: LocalDate)

           case class Purchase(
                     id: Int,
                     shippingInfoId: Int,
                     productId: Int,
                     count: Int,
                     total: Double
                   )

           val tables = (
             products = Table[Product]("product"),
             buyers = Table[Buyer]("buyers"),
             shipInfos = Table[ShippingInfo]("shippingInfo"),
             purchases = Table[Purchase]("purchase")
           )

          // TEST
          tables.products.withFilter(p => p.price != 0).sum(_.price).map(_) // should fail to compile because cannot call query methods on aggregation result
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class FlatmapExprCompileErrorTest extends munit.FunSuite {
  def testDescription: String = "flatMap without inner map fails"
  def expectedError: String = "Cannot return an Expr from a flatMap. Did you mean to use map?"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.{Table, Expr}
           import java.time.LocalDate

           case class Product(id: Int, name: String, price: Double)

           case class Buyer(id: Int, name: String, dateOfBirth: LocalDate)

           case class ShippingInfo(id: Int, buyerId: Int, shippingDate: LocalDate)

           case class Purchase(
                     id: Int,
                     shippingInfoId: Int,
                     productId: Int,
                     count: Int,
                     total: Double
                   )

           val tables = (
             products = Table[Product]("product"),
             buyers = Table[Buyer]("buyers"),
             shipInfos = Table[ShippingInfo]("shippingInfo"),
             purchases = Table[Purchase]("purchase")
           )

          // TEST
          tables.buyers.flatMap(b =>
            (bName = b.name, bId = b.id).toRow
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class FlatmapFlatmapCompileErrorTest extends munit.FunSuite {
  def testDescription: String = "flatMap without inner map fails"
  def expectedError: String = "Cannot return an Expr from a flatMap. Did you mean to use map?"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.{Table, Expr}
           import java.time.LocalDate

           case class Product(id: Int, name: String, price: Double)

           case class Buyer(id: Int, name: String, dateOfBirth: LocalDate)

           case class ShippingInfo(id: Int, buyerId: Int, shippingDate: LocalDate)

           case class Purchase(
                     id: Int,
                     shippingInfoId: Int,
                     productId: Int,
                     count: Int,
                     total: Double
                   )

           val tables = (
             products = Table[Product]("product"),
             buyers = Table[Buyer]("buyers"),
             shipInfos = Table[ShippingInfo]("shippingInfo"),
             purchases = Table[Purchase]("purchase")
           )

          // TEST
          tables.buyers.flatMap(b =>
            tables.shipInfos.flatMap(si =>
              (name = b.name, shippingDate = si.shippingDate).toRow
            )
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class MapFlatmapCompileErrorTest extends munit.FunSuite {
  def testDescription: String = "map with inner flatMap fails"
  def expectedError: String = "Cannot return an Expr from a flatMap. Did you mean to use map?"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.{Table, Expr}
           import java.time.LocalDate

           case class Product(id: Int, name: String, price: Double)

           case class Buyer(id: Int, name: String, dateOfBirth: LocalDate)

           case class ShippingInfo(id: Int, buyerId: Int, shippingDate: LocalDate)

           case class Purchase(
                     id: Int,
                     shippingInfoId: Int,
                     productId: Int,
                     count: Int,
                     total: Double
                   )

           val tables = (
             products = Table[Product]("product"),
             buyers = Table[Buyer]("buyers"),
             shipInfos = Table[ShippingInfo]("shippingInfo"),
             purchases = Table[Purchase]("purchase")
           )

          // TEST
          tables.buyers.map(b =>
            tables.shipInfos.flatMap(si =>
              (name = b.name, shippingDate = si.shippingDate).toRow
            )
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class AggregateWithoutAggregationCompileErrorTest extends munit.FunSuite {
  def testDescription: String = "aggregate that returns scalar expr should fail"
  def expectedError: String = "None of the overloaded alternatives of method aggregate" // TODO: can we force a better error message?

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.{Table, Expr}
           import java.time.LocalDate

           case class Product(id: Int, name: String, price: Double)

           case class Buyer(id: Int, name: String, dateOfBirth: LocalDate)

           case class ShippingInfo(id: Int, buyerId: Int, shippingDate: LocalDate)

           case class Purchase(
                     id: Int,
                     shippingInfoId: Int,
                     productId: Int,
                     count: Int,
                     total: Double
                   )

           val tables = (
             products = Table[Product]("product"),
             buyers = Table[Buyer]("buyers"),
             shipInfos = Table[ShippingInfo]("shippingInfo"),
             purchases = Table[Purchase]("purchase")
           )

          // TEST
          tables.products
            .aggregate(p => p.price)
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class AggregateFluentCompileErrorTest extends munit.FunSuite {
  def testDescription: String = "aggregate with further chaining of query methods shoudl fail"
  def expectedError: String = "map is not a member of tyql.Aggregation"

  test(testDescription) {
    val error: String =
      compileErrors(
        """
           // BOILERPLATE
           import language.experimental.namedTuples
           import tyql.{Table, Expr}
           import java.time.LocalDate

           case class Product(id: Int, name: String, price: Double)

           case class Buyer(id: Int, name: String, dateOfBirth: LocalDate)

           case class ShippingInfo(id: Int, buyerId: Int, shippingDate: LocalDate)

           case class Purchase(
                     id: Int,
                     shippingInfoId: Int,
                     productId: Int,
                     count: Int,
                     total: Double
                   )

           val tables = (
             products = Table[Product]("product"),
             buyers = Table[Buyer]("buyers"),
             shipInfos = Table[ShippingInfo]("shippingInfo"),
             purchases = Table[Purchase]("purchase")
           )

          // TEST
          tables.products.aggregate(p => (avgPrice = Expr.avg(p.price))).map(r => r == 10)
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class FlatmapAggregateTest4 extends SQLStringAggregationTest[AllCommerceDBs, Int] {
  def testDescription = "Flow: 2 nest, flatMap+flatMap should not fail because aggregation is Expr"
  def query() =
    testDB.tables.shipInfos.flatMap(si => // silly but correct syntax, equivalent to map + flatMap
      Expr.sum(si.buyerId)
    )

  def expectedQueryPattern = ""
}