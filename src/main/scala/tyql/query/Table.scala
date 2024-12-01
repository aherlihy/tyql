package tyql

import scala.deriving.Mirror

/** The type of query references to database tables */
case class Table[R] private ($name: String)(using r: ResultTag[R]) extends Query[R, BagResult]

object Table {
  def apply[R]()(using r: ResultTag[R], m: Mirror.Of[R], config: tyql.Config): Table[R] =
    new Table[R](config.caseConvention.convert(m.toString))
  def apply[R](name: String)(using r: ResultTag[R]): Table[R] =
    new Table[R](name)
  // TODO I dislike this () here. I would prefer something like Table[Products] and this would just summon the right thing without making any variables
}

// case class Database(tables: ) // TODO, do we need this?
