package tyql

import scala.Tuple.*
import scala.annotation.implicitNotFound

object Subset:
  /** NOTE: not currently used
   * Check if all the element types of X are also element types of Y
   *  /!\ Compile-time will be proportial to Length[X] * Length[Y].
   *
   * This is useful if we want to specify sort orders (or other query metadata)
   * via a tuple of (key: property) but want to make sure that the keys are
   * present in the provided tuple. Example: table.sort((key1: ASC, key2: DESC, ...))
   * instead of requiring 2 calls: table.sort(_.key1, ASC).sort(_.key2, DESC)
   */
  type Subset[X <: Tuple, Y <: Tuple] <: Boolean = X match
    case x *: xs => Contains[Y, x] match
      case true => Subset[xs, Y]
      case false => false
    case EmptyTuple => true

  @implicitNotFound("${X} contains types not in ${Y}")
  type IsSubset[X <: Tuple, Y <: Tuple] = Subset[X, Y] =:= true

  extension [T <: Tuple](t: T)
    def foo[S <: Tuple](s: S)(using IsSubset[S, T]) = {}


  val a: (Int, String, Double) = (1, "", 1.0)
  val b: (Double, String) = (2.0, " ")
  val b2: (Double, Double) = (2.0, 3.0)
  val b3: (Double, Float) = (2.0, 3.0f)

  a.foo(b) // ok

  a.foo(b2) // also ok

  // a.foo(b3) // error: (Double, Float) contains types not in (Int, String, Double)
