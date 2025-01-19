package tyql

import scala.deriving.Mirror
import scala.compiletime.constValueTuple
import scala.collection.immutable.LazyList.cons
import scala.annotation.implicitNotFound

/** The type of query references to database tables */
case class Table[R] private ($name: String)(using r: ResultTag[R]) extends Query[R, BagResult]
    with InsertableTable[R, NamedTuple.Names[NamedTuple.From[R]]] {
  type ColumnNames = NamedTuple.Names[NamedTuple.From[R]]
  type Types = NamedTuple.DropNames[NamedTuple.From[R]]
  override def underlyingTable = this

  def partial[Names <: Tuple]
    (using
        ev: TypeOperations.IsSubset[Names, NamedTuple.Names[NamedTuple.From[R]]],
    )
    : PartialTable[R, Names] =
    new PartialTable[R, Names](this)

  def delete(p: Expr.Ref[R, NonScalarExpr] => Expr[Boolean, NonScalarExpr]): Delete[R] =
    val ref = Expr.Ref[R, NonScalarExpr]()
    Delete(this, Expr.Fun(ref, p(ref)), Seq(), None)

  // update works similatly to inserts, there is a type-level check to ensure that the named tuple is assignable
  inline def update[N <: Tuple, T <: Tuple]
    (values: Expr.Ref[R, NonScalarExpr] => NamedTuple.NamedTuple[N, T])
    (using
        ev1: TypeOperations.IsSubset[N, NamedTuple.Names[NamedTuple.From[R]]],
        ev3: TypeOperations.IsAcceptableInsertion[
          Tuple.Map[T, Expr.StripExpr],
          TypeOperations.SelectByNames[
            N,
            NamedTuple.DropNames[NamedTuple.From[R]],
            NamedTuple.Names[NamedTuple.From[R]]
          ]
        ]
    )
    : Update[R] =
    val ref = Expr.Ref[R, NonScalarExpr]()
    val valuesTuple = values(ref)
    Update(this, constValueTuple[N].toList.asInstanceOf[List[String]], ref, valuesTuple.toList, None, Seq(), None)

  override def copyWith
    (
        requestedJoinType: Option[JoinType],
        requestedJoinOn: Option[Expr.Fun[R, Expr[Boolean, NonScalarExpr], NonScalarExpr]]
    )
    : Query[R, BagResult] =
    val ret = this.copy()
    ret.requestedJoinOn = requestedJoinOn
    ret.requestedJoinType = requestedJoinType
    ret
}

object Table {
  def apply[R]()(using r: ResultTag[R], m: Mirror.Of[R], config: tyql.Config): Table[R] =
    new Table[R](config.caseConvention.convert(m.toString))
  def apply[R](name: String)(using r: ResultTag[R]): Table[R] =
    new Table[R](name)
}

trait InsertableTable[R: ResultTag, Names <: Tuple] {

  def underlyingTable: Table[R]

  inline def insert(values: R*): Insert[R] = {
    val names = constValueTuple[NamedTuple.Names[NamedTuple.From[R]]].toList.asInstanceOf[List[String]]
    val valuesSeq = coerceTuplesIntoSeqSeq(values)
    Insert(underlyingTable, names, valuesSeq)
  }

  inline def insert[S <: Tuple]
    (values: S*)
    (using
        ev3: TypeOperations.IsAcceptableInsertion[
          Tuple.Map[S, Expr.StripExpr],
          TypeOperations.SelectByNames[
            Names,
            NamedTuple.DropNames[NamedTuple.From[R]],
            NamedTuple.Names[NamedTuple.From[R]]
          ]
        ]
    )
    : Insert[R] =
    Insert(underlyingTable, constValueTuple[Names].toList.asInstanceOf[List[String]], coerceTuplesIntoSeqSeq(values))

  inline def insert[N <: Tuple, T <: Tuple]
    (values: NamedTuple.NamedTuple[N, T]*)
    (using
        ev1: TypeOperations.IsSubset[N, Names],
        ev2: TypeOperations.IsSubset[Names, N],
        ev3: TypeOperations.IsAcceptableInsertion[
          Tuple.Map[
            TypeOperations.SelectByNames[Names, T, N],
            Expr.StripExpr
          ],
          TypeOperations.SelectByNames[
            Names,
            NamedTuple.DropNames[NamedTuple.From[R]],
            NamedTuple.Names[NamedTuple.From[R]]
          ]
        ]
    )
    : Insert[R] =
    Insert(underlyingTable, constValueTuple[N].toList.asInstanceOf[List[String]], coerceTuplesIntoSeqSeq(values))
}

case class PartialTable[R: ResultTag, Names <: Tuple](table: Table[R]) extends InsertableTable[R, Names] {
  override def underlyingTable = table
}

private def coerceTuplesIntoSeqSeq[S](values: Seq[S]): Seq[Seq[Any]] =
  (0 until values.length) map { (i: Int) =>
    val x = values(i)
    val list = x.asInstanceOf[Tuple].toList
    list
  }
