package test.query.flow
import test.SQLStringTest
import test.query.{commerceDBs,  AllCommerceDBs}

import tyql.*
import tyql.Expr.toRow
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

import java.time.LocalDate

class FlowTest1 extends SQLStringTest[AllCommerceDBs, (bName: String, bId: Int)] {
  def testDescription = "Flow: project tuple, 1 nest"
  def query() =
    for
      b <- testDB.tables.buyers
    yield (bName = b.name, bId = b.id).toRow

  def sqlString = """
      """
}

class FlowTest2 extends SQLStringTest[AllCommerceDBs, (name: String, shippingDate: LocalDate)] {
  def testDescription = "Flow: project tuple, 2 nest"
  def query() =
    for
      b <- testDB.tables.buyers
      si <- testDB.tables.shipInfos
    yield (name = b.name, shippingDate = si.shippingDate).toRow

  def sqlString = """
      """
}

class FlowTest3 extends SQLStringTest[AllCommerceDBs, String] {
  def testDescription = "Flow: single field, 1 nest"
  def query() =
    for
      b <- testDB.tables.buyers
    yield b.name

  def sqlString = """
      """
}

class FlowTest4 extends SQLStringTest[AllCommerceDBs, String] {
  def testDescription = "Flow: single field, 2 nest"
  def query() =
    for
      b <- testDB.tables.buyers
      si <- testDB.tables.shipInfos
    yield b.name

  def sqlString = """
      """
}