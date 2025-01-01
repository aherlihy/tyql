package tyql

import scala.Tuple.*
import scala.annotation.implicitNotFound
import scala.quoted.Type
import org.h2.command.query.Select

object Subset:
  /** NOTE: not currently used Check if all the element types of X are also element types of Y /!\ Compile-time will be
    * proportial to Length[X] * Length[Y].
    *
    * This is useful if we want to specify sort orders (or other query metadata) via a tuple of (key: property) but want
    * to make sure that the keys are present in the provided tuple. Example: table.sort((key1: ASC, key2: DESC, ...))
    * instead of requiring 2 calls: table.sort(_.key1, ASC).sort(_.key2, DESC)
    */
  type Kontains[X <: Tuple, Y] <: Boolean = X match
    case Y *: _ => true
    case _ *: xs => Kontains[xs, Y]
    case EmptyTuple => false
  type Subset[X <: Tuple, Y <: Tuple] <: Boolean = X match
    case x *: xs => Kontains[Y, x] match
        case true  => Subset[xs, Y]
        case false => false
    case EmptyTuple => true
  @implicitNotFound("${X} contains types not in ${Y}")
  type IsSubset[X <: Tuple, Y <: Tuple] = Subset[X, Y] =:= true

  extension [T <: Tuple](t: T)
    def fitsOver[S <: Tuple](s: S)(using IsSubset[S, T]) = {}

  type SelectTypeWithName[needle, Names, Types] = Names match
    case name *: _ => name match
        case needle => Tuple.Head[Types]
        case _ => SelectTypeWithName[needle, Tuple.Tail[Names], Tuple.Tail[Types]]
    case _ *: names => SelectTypeWithName[needle, names, Tuple.Tail[Types]]

  type CanBeAssigned[NamesIn <: Tuple, NamesOver <: Tuple, TypesIn <: Tuple, TypesOver <: Tuple] <: Boolean = NamesIn match
    case EmptyTuple => true
    case name *: namesInRest => Contains[NamesOver, name] match
        case false => false
        case true  => TypesIn match
           case t *: typesInRest => SelectTypeWithName[name, NamesOver, TypesOver] match
                case t => CanBeAssigned[namesInRest, NamesOver, typesInRest, TypesOver]
                case _ => false

  type SelectByNames[Names, OriginalTypes, OriginalNames] <: Tuple = Names match
    case name *: restNames => SelectTypeWithName[name, OriginalNames, OriginalTypes] *: SelectByNames[restNames, OriginalTypes, OriginalNames]
    case EmptyTuple => EmptyTuple

  type AcceptableInsertions[TypesA, TypesB] = TypesA match
    case Int *: restA => TypesB match
      case Option[Int] *: restB => AcceptableInsertions[restA, restB]
      case Option[Long] *: restB => AcceptableInsertions[restA, restB]
      case Int *: restB => AcceptableInsertions[restA, restB]
      case Long *: restB => AcceptableInsertions[restA, restB]
      case EmptyTuple => false
    case Double *: restA => TypesB match
      case Option[Double] *: restB => AcceptableInsertions[restA, restB]
      case Double *: restB => AcceptableInsertions[restA, restB]
      case EmptyTuple => false
    case Float *: restA => TypesB match
      case Option[Float] *: restB => AcceptableInsertions[restA, restB]
      case Option[Double] *: restB => AcceptableInsertions[restA, restB]
      case Float *: restB => AcceptableInsertions[restA, restB]
      case Double *: restB => AcceptableInsertions[restA, restB]
      case EmptyTuple => false
    case EmptyTuple => true

  type IsAcceptableInsertion[TypesA, TypesB] = AcceptableInsertions[TypesA, TypesB] =:= true
  type AlsoIsAcceptableInsertion[TypesA, TypesB] = AcceptableInsertions[TypesA, TypesB] =:= true

  val a: (Int, String, Double) = (1, "", 1.0)
  val b: (Double, String) = (2.0, " ")
  val b2: (Double, Double) = (2.0, 3.0)
  val b3: (Double, String, String) = (2.0, " ", " ")
  val startsWithFloat: (Float, String) = (2.0f, " ")

  val justOneString: Tuple1[String] = Tuple(" ")
  val twoStrings: (String, String) = (" ", " ")

  a.fitsOver(b) // ok

  a.fitsOver(b2) // also ok

  twoStrings.fitsOver(justOneString)

  justOneString.fitsOver(twoStrings) // error: Tuple1[String] contains types not in (String, String)

  type names112 = ("a", "b", "c")
  type needle112 = "c"
  type types112 = (Int, String, Double)
  type selectedType = SelectTypeWithName[needle112, names112, types112]
  val diditwork112: selectedType = 1.0

  type inNames8 = ("FIRST", "SECOND", "THIRD")
  type overNames8 = ("SECOND", "d", "THIRD", "a", "FIRST")
  type inTypes8 = (String, Double, Float)
  type overTypes8 = (Double, Int, /*3*/Float, Int, /*1*/String)
  type canBeAssigned8 = CanBeAssigned[inNames8, overNames8, inTypes8, overTypes8]

  // a.foo(b3) // error: (Double, Float) contains types not in (Int, String, Double)
