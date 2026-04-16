package tyql

import NamedTuple.NamedTuple

/** The type of query references to database tables. */
case class Table[R]($name: String)(using ResultTag[R])
    extends Query[R, BagResult]
    with InsertableTable[R, NamedTuple.Names[NamedTuple.From[R]]]:
  // -------- InsertableTable machinery (additive; see `InsertableTable` below) --------
  override def underlyingTable: Table[R] = this

  /** Narrow this table to a subset of its column names, for partial inserts
   *  where not every column is present in the source query.
   *
   *  e.g. `Table[(id: Int, name: String)].partial[("name" *: EmptyTuple)]`
   *  produces a `PartialTable` targeting only the `name` column. */
  def partial[Names <: Tuple]
    (using TypeOperations.IsSubset[Names, NamedTuple.Names[NamedTuple.From[R]]])
    : PartialTable[R, Names] =
    PartialTable[R, Names](this)

// case class Database(tables: ) // need seq of tables

/** Marker trait for anything that can appear as the target of `insertInto`.
 *  POC; the full `InsertableTable` trait with literal-value `insert(...)`
 *  overloads lives on the `backend-specialization` branch. */
trait InsertableTable[R, Names <: Tuple]:
  def underlyingTable: Table[R]

/** A [[Table]] restricted to a subset of its columns — produced by
 *  [[Table.partial]].  Used when the source query in `insertInto` populates
 *  only some of the table's columns. */
case class PartialTable[R: ResultTag, Names <: Tuple](table: Table[R])
    extends InsertableTable[R, Names]:
  override def underlyingTable: Table[R] = table
