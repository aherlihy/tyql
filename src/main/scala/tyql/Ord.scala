package tyql

import language.experimental.namedTuples
import NamedTuple.{NamedTuple, AnyNamedTuple}


trait Ord {
  case object ASC
  case object DESC
}
// express named tuple with any # elements but all type ORD?
// restrict keys to existing keys?

// Problem: Fields is on Expr, but this is a query
type Ordering[B, A <: Query[B]] = NamedTuple[NamedTuple.Names[A.Fields], Tuple.Map[NamedTuple.DropNames[A.Fields], Ord]] // TODO: want a tuple of lengh A, but only type Ord