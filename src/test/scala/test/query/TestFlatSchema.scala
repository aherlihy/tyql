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

case class Purchase
  (
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

  override def init(): String =
    """
CREATE TABLE Product (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    price NUMERIC
);

CREATE TABLE Buyer (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    date_of_birth DATE
);

CREATE TABLE ShippingInfo (
    id SERIAL PRIMARY KEY,
    buyer_id INT,
    shipping_date DATE,
    FOREIGN KEY (buyer_id) REFERENCES Buyer(id)
);

CREATE TABLE Purchase (
    id SERIAL PRIMARY KEY,
    shipping_info_id INT,
    product_id INT,
    count INT,
    total NUMERIC,
    FOREIGN KEY (shipping_info_id) REFERENCES ShippingInfo(id),
    FOREIGN KEY (product_id) REFERENCES Product(id)
);


INSERT INTO Product (id, name, price)
VALUES
(1, 'Laptop', 1200.00),
(2, 'Smartphone', 800.00),
(3, 'Tablet', 500.00);

INSERT INTO Buyer (id, name, date_of_birth)
VALUES
(1, 'John Doe', '1985-03-15'),
(2, 'Jane Smith', '1990-06-25'),
(3, 'Alice Johnson', '1978-12-30');

INSERT INTO ShippingInfo (id, buyer_id, shipping_date)
VALUES
(1, 1, '2024-01-05'),
(2, 2, '2024-01-08'),
(3, 3, '2024-01-10');

INSERT INTO Purchase (id, shipping_info_id, product_id, count, total)
VALUES
(1, 1, 1, 1, 1200.00),
(2, 2, 2, 2, 1600.00),
(3, 3, 3, 1, 500.00);
"""
