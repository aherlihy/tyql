package test.query.project
import test.SQLStringTest
import test.query.{commerceDBs,  AllCommerceDBs}

import tyql.*
import tyql.Expr.toRow
import language.experimental.namedTuples
import NamedTuple.*
import scala.language.implicitConversions

import java.time.LocalDate

class ReturnIntTest extends SQLStringTest[AllCommerceDBs, Int] {
  def testDescription = "Project: return int"
  def query() =
    testDB.tables.products.map: c =>
      c.id

  def sqlString = """
      """
}

class ReturnStringTest extends SQLStringTest[AllCommerceDBs, String] {
  def testDescription = "Project: return string"
  def query() =
    testDB.tables.products.map: c =>
      c.name

  def sqlString = """
      """
}

class ProjectIntTest extends SQLStringTest[AllCommerceDBs, (id: Int)] {
  def testDescription = "Project: project int"
  def query() =
    testDB.tables.products.map: c =>
      (id = c.id).toRow

  def sqlString = """
      """
}

class ProjectStringTest extends SQLStringTest[AllCommerceDBs, (name: String)] {
  def testDescription = "Project: project string"
  def query() =
    testDB.tables.products.map: c =>
      (name = c.name).toRow

  def sqlString = """
      """
}

class ProjectMixedTest extends SQLStringTest[AllCommerceDBs, (name: String, id: Int)] {
  def testDescription = "Project: project string+int"
  def query() =
    testDB.tables.products.map: c =>
      (name = c.name, id = c.id).toRow

  def sqlString = """
      """
}

class ProjectString2Test extends SQLStringTest[AllCommerceDBs, (name: String, name2: String)] {
  def testDescription = "Project: project string+string"
  def query() =
    testDB.tables.products.map: c =>
      (name = c.name, name2 = c.name).toRow

  def sqlString = """
      """
}

class JoinProjectInt2Test extends SQLStringTest[AllCommerceDBs, (id: Int, id2: Int)] {
  def testDescription = "Project: project int+int"
  def query() =
    testDB.tables.products.map: c =>
      (id = c.id, id2 = c.id).toRow

  def sqlString = """
      """
}