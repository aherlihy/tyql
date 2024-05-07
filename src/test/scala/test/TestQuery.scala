package test // test package so that it can be imported by bench.
import tyql.*

class TestTable[Row] {
  def table: Table[Row] = Table[Row]("tableName")
  def init(): Unit = ???
}
// class CitiesTable extends TestTable[CityT] in case custom init method required

trait TestQuery[Row, Return](using val testTable: TestTable[Row]) {
  def testDescription: String
  def query(): Query[Return]
  def sqlString: String
}


abstract class SQLStringTest[Row, Return](using TestTable[Row]) extends munit.FunSuite with TestQuery[Row, Return] {
  test(testDescription) {
    assertEquals(query().toSQLString, sqlString)
  }
  // TODO: test other features
}