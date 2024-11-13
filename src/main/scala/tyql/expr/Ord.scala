package tyql

import language.experimental.namedTuples
import NamedTuple.{NamedTuple, AnyNamedTuple}

sealed trait Ord
object Ord:
  case object ASC extends Ord
  case object DESC extends Ord
