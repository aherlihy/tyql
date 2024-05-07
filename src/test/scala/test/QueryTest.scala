package test // test package so that it can be imported by bench. TODO: better way to structure?
import tyql.*

trait TestTable[Row] {
  def table: Table[Row] = Table[Row]("tableName")
  def init(): Unit = ???
}
// class CitiesTable extends TestTable[CityT] in case custom init method required

trait TestQuery[Row, Return] extends TestTable[Row] {
  def testDescription: String
  def query(): Query[Return]
  def sqlString: String
}

abstract class SQLStringTest[Row, Return] extends munit.FunSuite with TestQuery[Row, Return] {
  test(testDescription) {
    assert(query().toSQLString == sqlString)
  }
  // TODO: test other features
}


