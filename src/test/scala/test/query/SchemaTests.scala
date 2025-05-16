package test.query.schema

import munit.FunSuite
import test.query.{AllCommerceDBs, commerceDBs}
import tyql.*
import tyql.ResultTag.*
import tyql.Expr.{IntLit, StringLit, toRow}

import language.experimental.namedTuples
import NamedTuple.*
import java.time.LocalDate

class QueryIRSchemaSuite extends FunSuite {
  import tyql.TreePrettyPrinter.*

  val testDB = commerceDBs

  test("Project: return Int") {
    val q = testDB.tables.products.map(p => IntLit(1))
    assertEquals(q.toQueryIR.schema, IntTag)
  }

  test("Project: return String") {
    val q = testDB.tables.products.map(p => StringLit("test"))
    assertEquals(q.toQueryIR.schema, StringTag)
  }

  test("Project: single named field") {
    val q = testDB.tables.products.map(p => (id = p.id).toRow)
    assertEquals(q.toQueryIR.schema, NamedTupleTag(List("id"), List(IntTag)))
  }

  test("Project: mixed named fields") {
    val q = testDB.tables.products.map(p => (name = p.name, id = p.id).toRow)
    assertEquals(q.toQueryIR.schema, NamedTupleTag(List("name", "id"), List(StringTag, IntTag)))
  }

  test("Project: duplicate named string fields") {
    val q = testDB.tables.products.map(p => (name = p.name, name2 = p.name).toRow)
    assertEquals(q.toQueryIR.schema, NamedTupleTag(List("name", "name2"), List(StringTag, StringTag)))
  }

  test("Project: project entire row") {
    val q = testDB.tables.buyers.map(p => p)
    println(s"AST=${q.prettyPrint(0)}")
    println(s"IR=${q.toQueryIR.prettyPrintIR(0, true)}")
    val prod = NamedTupleTag(List("id", "name", "dateOfBirth"), List(IntTag, StringTag, LocalDateTag))
    assertEquals(ResultTag.extractProduct(q.toQueryIR.schema), prod)
  }

  test("Project: project to named tuple of 3") {
    val q = testDB.tables.products.map(p =>
      (id = p.id, name = p.name, price = p.price).toRow
    )
    assertEquals(q.toQueryIR.schema, NamedTupleTag(List("id", "name", "price"), List(IntTag, StringTag, DoubleTag)))
  }

  test("Project: concat projections") {
    val tupleProd = testDB.tables.products.map(p =>
      (id = p.id, name = p.name, price = p.price).toRow
    )
    val tupleShip = testDB.tables.shipInfos.map(s =>
      (buyerId = s.buyerId, shippingDate = s.shippingDate).toRow
    )

    val q = for
      p <- tupleProd
      s <- tupleShip
    yield p.concat(s)

    val expected = NamedTupleTag(
      List("id", "name", "price", "buyerId", "shippingDate"),
      List(IntTag, StringTag, DoubleTag, IntTag, LocalDateTag)
    )

    assertEquals(q.toQueryIR.schema, expected)
  }
}

class QueryIRMergeBehaviorSuite extends FunSuite {

  val testDB = commerceDBs

  test("Merge: map-join map merges product and buyer schema") {
    val q = for
      p <- testDB.tables.products.map(p =>
        (id = p.id, name = p.name, price = p.price).toRow
      )
      b <- testDB.tables.buyers.map(b =>
        (buyerId = b.id, dob = b.dateOfBirth).toRow
      )
    yield p.concat(b)

    val expected = NamedTupleTag(
      List("id", "name", "price", "buyerId", "dob"),
      List(IntTag, StringTag, DoubleTag, IntTag, LocalDateTag)
    )

    assertEquals(q.toQueryIR.schema, expected)
  }

  test("Merge: map-join filter returns merged schema with correct types") {
    val q = for
      p <- testDB.tables.products.map(p =>
        (id = p.id, name = p.name).toRow
      )
      s <- testDB.tables.shipInfos.filter(s => s.shippingDate == 1 )
    yield p.concat((shipYear = s.shippingDate).toRow)

    val expected = NamedTupleTag(
      List("id", "name", "shipYear"),
      List(IntTag, StringTag, LocalDateTag)
    )

    assertEquals(q.toQueryIR.schema, expected)
  }

  test("Merge: map-join ordered query preserves merged schema") {
    val productsOrdered = testDB.tables.products
      .sort(p => p.price, Ord.ASC)
      .map(p => (id = p.id).toRow)

    val buyers = testDB.tables.buyers.map(b => (name = b.name).toRow)

    val q = for
      p <- productsOrdered
      b <- buyers
    yield p.concat(b)

    val expected = NamedTupleTag(
      List("id", "name"),
      List(IntTag, StringTag)
    )

    assertEquals(q.toQueryIR.schema, expected)
  }

  test("Merge: join with distinct query yields merged schema") {
    val distinctBuyers = testDB.tables.buyers
      .distinct
      .map(b => (name = b.name).toRow)

    val q = for
      b1 <- testDB.tables.buyers.map(b => (id = b.id).toRow)
      b2 <- distinctBuyers
    yield b1.concat(b2)

    val expected = NamedTupleTag(
      List("id", "name"),
      List(IntTag, StringTag)
    )

    assertEquals(q.toQueryIR.schema, expected)
  }

}


class QueryIRComplexSuite extends FunSuite {

  val testDB = commerceDBs

  test("FlatMap-collapse: triple join via flatMap") {
    val q =
      testDB.tables.products.flatMap(p =>
        testDB.tables.buyers.flatMap(b =>
          testDB.tables.purchases.map(o =>
            (prodId = p.id, buyerName = b.name, orderId = o.id).toRow
          )
        )
      )

    val expected = NamedTupleTag(
      List("prodId", "buyerName", "orderId"),
      List(IntTag, StringTag, IntTag)
    )

    assertEquals(q.toQueryIR.schema, expected)
  }

  test("Sort-collapse: multi-level sort collapse") {
    val sorted =
      testDB.tables.products
        .sort(p => p.price, Ord.ASC)
        .sort(p => p.id, Ord.DESC)
        .map(p => (id = p.id, price = p.price).toRow)

    val expected = NamedTupleTag(List("id", "price"), List(IntTag, DoubleTag))
    assertEquals(sorted.toQueryIR.schema, expected)
  }

  test("Filter-collapse: multi-filter flattening") {
    val q =
      testDB.tables.buyers
        .filter(b => b.id > 0)
        .filter(b => b.name != "")
        .map(b => (id = b.id).toRow)

    val expected = NamedTupleTag(List("id"), List(IntTag))
    assertEquals(q.toQueryIR.schema, expected)
  }

  test("Set operation: union") {
    val buyers1 = testDB.tables.buyers.map(b => (id = b.id).toRow)
    val buyers2 = testDB.tables.buyers.map(b => (id = b.id).toRow)
    val q = buyers1.union(buyers2)

    val expected = NamedTupleTag(List("id"), List(IntTag))
    assertEquals(q.toQueryIR.schema, expected)
  }

  test("Set operation: intersectAll with distinct schema") {
    val ship1 = testDB.tables.shipInfos.map(s => (buyerId = s.buyerId).toRow)
    val ship2 = testDB.tables.shipInfos.map(s => (buyerId = s.buyerId).toRow)
    val q = ship1.intersectAll(ship2)

    val expected = NamedTupleTag(List("buyerId"), List(IntTag))
    assertEquals(q.toQueryIR.schema, expected)
  }

  test("Offset + limit: preserved schema") {
    val q = testDB.tables.products
      .map(p => (id = p.id, name = p.name).toRow)
      .drop(10)
      .take(5)

    val expected = NamedTupleTag(List("id", "name"), List(IntTag, StringTag))
    assertEquals(q.toQueryIR.schema, expected)
  }

  test("Distinct on map: no schema change") {
    val q = testDB.tables.buyers
      .map(b => (id = b.id, name = b.name).toRow)
      .distinct

    val expected = NamedTupleTag(List("id", "name"), List(IntTag, StringTag))
    assertEquals(q.toQueryIR.schema, expected)
  }

  test("Multi-level nested map with flatMap collapse") {
    val q =
      testDB.tables.products.flatMap(p =>
        testDB.tables.buyers.map(b =>
          (productName = p.name, buyerName = b.name).toRow
        )
      ).map(row =>
        (name = row.productName).toRow
      )

    val expected = NamedTupleTag(List("name"), List(StringTag))
    assertEquals(q.toQueryIR.schema, expected)
  }

  test("Nested join with exists subquery (isEmpty)") {
    val q =
      testDB.tables.buyers.map(b =>
        (id = b.id, hasShip = testDB.tables.shipInfos.filter(s => s.buyerId == b.id).isEmpty).toRow
      )

    val expected = NamedTupleTag(List("id", "hasShip"), List(IntTag, BoolTag))
    assertEquals(q.toQueryIR.schema, expected)
  }
}
