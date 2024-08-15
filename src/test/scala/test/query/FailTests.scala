package test.query.fail

class MapMapCompileErrorTest extends munit.FunSuite {
  def testDescription: String = "map+map should fail with useful error message"
  def expectedError: String = "Cannot return an Query from a map. Did you mean to use flatMap?"

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
  def expectedError: String = "Cannot return an Query from a map. Did you mean to use flatMap?"

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
  def expectedError: String = "Cannot return an Query from a map. Did you mean to use flatMap?"

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

class FlatmapOnlyCompileErrorTest extends munit.FunSuite {
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

class AggregateCompileErrorTest extends munit.FunSuite {
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

class AggregateSubqueryCompileErrorTest extends munit.FunSuite {
  def testDescription: String = "aggregate with aggregate subquery as source should fail"
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
