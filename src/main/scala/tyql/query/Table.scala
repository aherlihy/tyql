package tyql

import scala.deriving.Mirror
import scala.compiletime.constValueTuple
import scala.collection.immutable.LazyList.cons

/** The type of query references to database tables */
case class Table[R] private ($name: String)(using r: ResultTag[R]) extends Query[R, BagResult]
    with InsertableTable[R, NamedTuple.Names[NamedTuple.From[R]]] {
  type ColumnNames = NamedTuple.Names[NamedTuple.From[R]]
  type Types = NamedTuple.DropNames[NamedTuple.From[R]]
  inline def columnNames: Tuple = constValueTuple[ColumnNames]

  def partial[Names <: Tuple]
    (using ev: Subset.IsSubset[Names, NamedTuple.Names[NamedTuple.From[R]]])
    : PartialTable[R, Names] =
    new PartialTable[R, Names](this)

  override def underlyingTable = this
}

object Table {
  def apply[R]()(using r: ResultTag[R], m: Mirror.Of[R], config: tyql.Config): Table[R] =
    new Table[R](config.caseConvention.convert(m.toString))
  def apply[R](name: String)(using r: ResultTag[R]): Table[R] =
    new Table[R](name)
}

trait InsertableTable[R: ResultTag, Names <: Tuple] {

  def underlyingTable: Table[R]

  private def coerceTuplesIntoSeqSeq[S](values: Seq[S]): Seq[Seq[Any]] =
    (0 until values.length) map { (i: Int) =>
      val x = values(i)
      val list = x.asInstanceOf[Tuple].toList
      list
    }

  inline def insert[S <: Tuple]
    (values: S*)
    (using
        ev3: Subset.AlsoIsAcceptableInsertion[
          Tuple.Map[S, Expr.StripExpr],
          Subset.SelectByNames[Names, NamedTuple.DropNames[NamedTuple.From[R]], NamedTuple.Names[NamedTuple.From[R]]]
        ]
    )
    : Insert[R] = Insert(underlyingTable, constValueTuple[Names].toList.asInstanceOf[List[String]], coerceTuplesIntoSeqSeq(values))

  inline def insert[N <: Tuple, T <: Tuple]
    (values: NamedTuple.NamedTuple[N, T]*)
    (using
        ev1: Subset.IsSubset[N, Names],
        ev2: Subset.IsSubset[Names, N],
        ev3: Subset.IsAcceptableInsertion[
          Tuple.Map[
            Subset.SelectByNames[Names, T, N],
            Expr.StripExpr
          ],
          Subset.SelectByNames[Names, NamedTuple.DropNames[NamedTuple.From[R]], NamedTuple.Names[NamedTuple.From[R]]]
        ]
    )
    : Insert[R] = Insert(underlyingTable, constValueTuple[N].toList.asInstanceOf[List[String]], coerceTuplesIntoSeqSeq(values))
}

case class PartialTable[R: ResultTag, Names <: Tuple](table: Table[R]) extends InsertableTable[R, Names] {
  override def underlyingTable = table
}
