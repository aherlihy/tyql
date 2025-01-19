package tyql

import language.experimental.namedTuples
import scala.language.implicitConversions
import NamedTuple.{AnyNamedTuple, NamedTuple}
import tyql.*
import scala.annotation.implicitNotFound

@main def main(): Unit =
  driverMain()
