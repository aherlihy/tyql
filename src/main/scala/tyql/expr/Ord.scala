package tyql

import language.experimental.namedTuples
import NamedTuple.{NamedTuple, AnyNamedTuple}

trait Ord
object Ord:
  case object ASC extends Ord
  case object DESC extends Ord

// express named tuple with subset of keys of T but all type ORD?
// Problem: Fields is on Expr, but this is a query
// type Ordering[A <: AnyNamedTuple] = NamedTuple[NamedTuple.Names[A], Tuple.Map[NamedTuple.DropNames[A], Ord]] // TODO: want a tuple of lengh A, but only type Ord