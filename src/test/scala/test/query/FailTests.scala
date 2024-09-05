package test.query.fail

import test.SQLStringAggregationTest
import tyql.*
import test.query.{commerceDBs, AllCommerceDBs}
import language.experimental.namedTuples

class MissingAttributeCompileErrorTest extends munit.FunSuite {
  def testDescription: String = "named tuples require attributes to exist"
  def expectedError: String = "doesNotExist is not a member of"

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
             tables.shipInfos.map(si =>
               (name = b.name, shippingDate = si.doesNotExist).toRow
             )
        """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class MapMapProjectCompileErrorTest extends munit.FunSuite {
  def testDescription: String = "map+map should fail with useful error message"
  def expectedError: String = "None of the overloaded alternatives"

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

class MapMapToRowCompileErrorTest extends munit.FunSuite {
  def testDescription: String = "map+map with toRow should fail with useful error message"
  def expectedError: String = "None of the overloaded alternatives"

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
  def expectedError: String = "None of the overloaded alternatives"

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

class MapMapAggregateProjectCompileErrorTest extends munit.FunSuite {
  def testDescription: String = "map+map with aggregate should fail with useful error message"
  def expectedError: String = "None of the overloaded alternatives"

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
              (e = Expr.sum(si.buyerId)).toRow
            )
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class AggregateFluentMapCompileErrorTest extends munit.FunSuite {
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
  def expectedError: String = "None of the overloaded alternatives"

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
  def expectedError: String = "None of the overloaded alternatives"

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
  def expectedError: String = "None of the overloaded alternatives"

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
  def expectedError: String = "None of the overloaded alternatives"

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

class AggregateWithoutAggregationProjectCompileErrorTest extends munit.FunSuite {
  def testDescription: String = "aggregate that returns a projected scalar expr should fail"
  def expectedError: String = "None of the overloaded alternatives"

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
          import AggregationExpr.toRow
          tables.products
            .aggregate(p => (pr = p.price, pid = p.id).toRow)
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class AggregateFluentFilterCompileErrorTest extends munit.FunSuite {
  def testDescription: String = "aggregate with further chaining of query methods should fail"
  def expectedError: String = "filter is not a member of tyql.Aggregation"

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
          tables.products.aggregate(p => (avgPrice = Expr.avg(p.price))).filter(r => r == 10)
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class AggregateInMapErrorTest extends munit.FunSuite {
  def testDescription: String = "aggregation inside map should fail"
  def expectedError: String = "None of the overloaded alternatives"

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
          tables.shipInfos.map(si =>
            Expr.sum(si.buyerId)
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class AggregateInMapProjectErrorTest extends munit.FunSuite {
  def testDescription: String = "aggregation inside map should fail"
  def expectedError: String = "None of the overloaded alternatives"

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
          tables.shipInfos.map(si =>
            (s = Expr.sum(si.buyerId)).toRow
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class AggregateInMapSubexpressionErrorTest extends munit.FunSuite {
  def testDescription: String = "aggregation inside map should fail, even inside expression with non-agg"
  def expectedError: String = "None of the overloaded alternatives"

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
          import AggregationExpr.toRow
          tables.shipInfos.map(si =>
            (s = si.id == Expr.sum(si.buyerId)).toRow
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class AggregateInFlatmapErrorTest extends munit.FunSuite {
  def testDescription: String = "aggregation inside flatmap should fail"
  def expectedError: String = "None of the overloaded alternatives"

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
          tables.shipInfos.flatMap(si =>
            Expr.sum(si.buyerId)
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class AggregateInFlatmapProjectErrorTest extends munit.FunSuite {
  def testDescription: String = "aggregation inside flatmap should fail"
  def expectedError: String = "None of the overloaded alternatives"

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
          tables.shipInfos.flatMap(si =>
            (e = Expr.sum(si.buyerId)).toRow
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class AggregateInNestedFlatmapErrorTest extends munit.FunSuite {
  def testDescription: String = "aggregation nested inside flatmap should fail"
  def expectedError: String = "None of the overloaded alternatives"

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
            tables.shipInfos.aggregate(si =>
              Expr.sum(si.buyerId)
            )
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class MapInNestedAggregateErrorTest extends munit.FunSuite {
  def testDescription: String = "map nested inside aggregate should fail"
  def expectedError: String = "None of the overloaded alternatives"

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
          import AggregationExpr.toRow
          import Expr.toRow
          tables.buyers.aggregate(b =>
            tables.shipInfos.map(si =>
              (newId = si.id, bId = b.id).toRow
            )
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class MixedProjectAggregateErrorTest extends munit.FunSuite {
  def testDescription: String = "should not be able to project aggregates + non-aggregate fields in aggregate result"
  def expectedError: String = "value toRow is not a member of"

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
          import tyql.AggregationExpr.toRow
          import tyql.Expr.toRow
          tables.buyers.aggregate(b =>
            (maxbId=Expr.max(b.id), bId = b.id).toRow
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}

class MixedProjectMapErrorTest extends munit.FunSuite {
  def testDescription: String = "should not be able to project aggregates + non-aggregate fields in map result"
  def expectedError: String = "value toRow is not a member of"

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
            (maxbId=Expr.max(b.id), bId = b.id).toRow
          )
          """)
    assert(error.contains(expectedError), s"Expected substring '$expectedError' in '$error'")
  }
}



