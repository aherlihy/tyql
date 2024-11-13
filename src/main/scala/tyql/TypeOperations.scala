package tyql

import scala.Tuple.*
import scala.annotation.implicitNotFound
import scala.quoted.Type

object TypeOperations:
  type Kontains[X <: Tuple, Y] <: Boolean = X match
    case Y *: _     => true
    case _ *: xs    => Kontains[xs, Y]
    case EmptyTuple => false
  type Subset[X <: Tuple, Y <: Tuple] <: Boolean = X match
    case x *: xs => Kontains[Y, x] match
        case true  => Subset[xs, Y]
        case false => false
    case EmptyTuple => true
  @implicitNotFound("${X} contains types not in ${Y}")
  type IsSubset[X <: Tuple, Y <: Tuple] = Subset[X, Y] =:= true

  @implicitNotFound("${Y} cannot be found inside ${X}")
  type DoesContain[X <: Tuple, Y] = Kontains[X, Y] =:= true

  type SelectTypeWithName[needle, Names, Types] = Names match
    case name *: _ => name match
        case needle => Tuple.Head[Types]
        case _      => SelectTypeWithName[needle, Tuple.Tail[Names], Tuple.Tail[Types]]
    case _ *: names => SelectTypeWithName[needle, names, Tuple.Tail[Types]]

  type CanBeAssigned[NamesIn <: Tuple, NamesOver <: Tuple, TypesIn <: Tuple, TypesOver <: Tuple] <: Boolean =
    NamesIn match
      case EmptyTuple => true
      case name *: namesInRest => Contains[NamesOver, name] match
          case false => false
          case true => TypesIn match
              case t *: typesInRest => SelectTypeWithName[name, NamesOver, TypesOver] match
                  case t => CanBeAssigned[namesInRest, NamesOver, typesInRest, TypesOver]
                  case _ => false

  type SelectByNames[Names, OriginalTypes, OriginalNames] <: Tuple = Names match
    case name *: restNames =>
      SelectTypeWithName[name, OriginalNames, OriginalTypes] *: SelectByNames[restNames, OriginalTypes, OriginalNames]
    case EmptyTuple => EmptyTuple

  // XXX Scala compiler will INCORRECTLY compute this if you change it even a little, this is the only working version after 3+ hours of trying
  // XXX do not split this into helper types, do not use nested type matching, do not abstract it, only extend this by example
  type AcceptableInsertions[TypesA <: Tuple, TypesB <: Tuple] <: Boolean = (TypesA, TypesB) match
    case (EmptyTuple, EmptyTuple)                     => true
    case (Int *: restA, Int *: restB)                 => AcceptableInsertions[restA, restB]
    case (Int *: restA, Option[Int] *: restB)         => AcceptableInsertions[restA, restB]
    case (Int *: restA, Long *: restB)                => AcceptableInsertions[restA, restB]
    case (Int *: restA, Option[Long] *: restB)        => AcceptableInsertions[restA, restB]
    case (Long *: restA, Long *: restB)               => AcceptableInsertions[restA, restB]
    case (Long *: restA, Option[Long] *: restB)       => AcceptableInsertions[restA, restB]
    case (Double *: restA, Double *: restB)           => AcceptableInsertions[restA, restB]
    case (Double *: restA, Option[Double] *: restB)   => AcceptableInsertions[restA, restB]
    case (Float *: restA, Double *: restB)            => AcceptableInsertions[restA, restB]
    case (Float *: restA, Option[Double] *: restB)    => AcceptableInsertions[restA, restB]
    case (Float *: restA, Float *: restB)             => AcceptableInsertions[restA, restB]
    case (Float *: restA, Option[Float] *: restB)     => AcceptableInsertions[restA, restB]
    case (Boolean *: restA, Boolean *: restB)         => AcceptableInsertions[restA, restB]
    case (Boolean *: restA, Option[Boolean] *: restB) => AcceptableInsertions[restA, restB]
    case (String *: restA, String *: restB)           => AcceptableInsertions[restA, restB]
    case (String *: restA, Option[String] *: restB)   => AcceptableInsertions[restA, restB]
    case _                                            => false

  type IsAcceptableInsertion[TypesA <: Tuple, TypesB <: Tuple] = AcceptableInsertions[TypesA, TypesB] =:= true
