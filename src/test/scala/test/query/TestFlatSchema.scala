package test.query
import test.*
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

type AllCommerceDBs = (products: Product, buyers: Buyer, shipInfos: ShippingInfo, purchases: Purchase)

// Test databases
given commerceDBs: TestDatabase[AllCommerceDBs] with
  override def tables = (
    products = Table[Product]("product"),
    buyers = Table[Buyer]("buyers"),
    shipInfos = Table[ShippingInfo]("shippingInfo"),
    purchases = Table[Purchase]("purchase")
  )