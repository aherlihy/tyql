package test
import tyql.*
import language.experimental.namedTuples
import NamedTuple.*

import java.time.LocalDate

// Row Types without nested data
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

// Test databases
given commerceDBs: TestDatabase[(Product, Buyer, ShippingInfo, Purchase)] with
  override def tables = (
    Table[Product]("product"),
    Table[Buyer]("buyers"),
    Table[ShippingInfo]("shippingInfo"),
    Table[Purchase]("purchase")
  )

given buyerShipDB: TestDatabase[(Buyer, ShippingInfo)] with
  override def tables = (
    Table[Buyer]("buyers"),
    Table[ShippingInfo]("shippingInfo")
  )