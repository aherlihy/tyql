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

  override def init(): String =
    """
CREATE DATABASE IF NOT EXISTS AllCommerceDB;
use AllCommerceDB;
DROP TABLE IF EXISTS product;
DROP TABLE IF EXISTS purchase;
DROP TABLE IF EXISTS buyers;
DROP TABLE IF EXISTS shippingInfo;


CREATE TABLE product (
    id INT,
    name VARCHAR(255),
    price NUMERIC
);

CREATE TABLE buyers (
    id INT,
    name VARCHAR(255),
    dateOfBirth DATE
);

CREATE TABLE shippingInfo (
    id INT,
    shippingDate DATE,
    buyerId INT
);

CREATE TABLE purchase (
    id INT,
    shippingInfoId INT,
    productId INT,
    count INT,
    total NUMERIC
);


INSERT INTO product (id, name, price)
VALUES
(1, 'Laptop', 1200.00),
(2, 'Smartphone', 800.00),
(3, 'Tablet', 500.00);

INSERT INTO buyers (id, name, dateOfBirth)
VALUES
(1, 'John Doe', '1985-03-15'),
(2, 'Jane Smith', '1990-06-25'),
(3, 'Alice Johnson', '1978-12-30');

INSERT INTO shippingInfo (id, buyerId, shippingDate)
VALUES
(1, 1, '2024-01-05'),
(2, 2, '2024-01-08'),
(3, 3, '2024-01-10');

INSERT INTO purchase (id, shippingInfoId, productId, count, total)
VALUES
(1, 1, 1, 1, 1200.00),
(2, 2, 2, 2, 1600.00),
(3, 3, 3, 1, 500.00);
"""
