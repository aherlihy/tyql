package tyql

import java.time.LocalDate
import scala.NamedTuple.NamedTuple
import scala.compiletime.{constValue, constValueTuple, summonAll}
import scala.deriving.Mirror

enum ResultTag[T]:
  case NullTag extends ResultTag[scala.Null]
  case IntTag extends ResultTag[Int]
  case DoubleTag extends ResultTag[Double]
  case StringTag extends ResultTag[String]
  case BoolTag extends ResultTag[Boolean]
  case LocalDateTag extends ResultTag[LocalDate]
  // names is a var to special case when we want to treat a named tuple like a regular tuple without going through type conversion
  case NamedTupleTag[N <: Tuple, V <: Tuple](var names: List[String], types: List[ResultTag[?]])
      extends ResultTag[NamedTuple[N, V]]
//  case TupleTag[T <: Tuple](types: List[ResultTag[?]]) extends ResultTag[Tuple]
  case ProductTag[T](productName: String, fields: ResultTag[NamedTuple.From[T]], m: Mirror.ProductOf[T])
      extends ResultTag[T]
  case ListTag[T](elementType: ResultTag[T]) extends ResultTag[List[T]]
  case OptionalTag[T](elementType: ResultTag[T]) extends ResultTag[Option[T]]
  case AnyTag extends ResultTag[Any]
// TODO: Add more types, specialize for DB backend
object ResultTag:
  given ResultTag[scala.Null] = ResultTag.NullTag
  given ResultTag[Int] = ResultTag.IntTag
  given ResultTag[String] = ResultTag.StringTag
  given ResultTag[Boolean] = ResultTag.BoolTag
  given ResultTag[Double] = ResultTag.DoubleTag
  given ResultTag[LocalDate] = ResultTag.LocalDateTag
  given [T](using e: ResultTag[T]): ResultTag[Option[T]] = ResultTag.OptionalTag(e)
//  inline given [T <: Tuple]: ResultTag[Tuple] =
//    val tpes = summonAll[Tuple.Map[T, ResultTag]]
//    TupleTag(tpes.toList.asInstanceOf[List[ResultTag[?]]])
  inline given [N <: Tuple, V <: Tuple]: ResultTag[NamedTuple[N, V]] =
    val names = constValueTuple[N]
    val tpes = summonAll[Tuple.Map[V, ResultTag]]
    NamedTupleTag(names.toList.asInstanceOf[List[String]], tpes.toList.asInstanceOf[List[ResultTag[?]]])

  // We don't really need `fields` and could use `m` for everything, but maybe we can share a cached
  // version of `fields`.
  // Alternatively if we don't care about the case class name we could use only `fields`.
  inline given [T](using m: Mirror.ProductOf[T], fields: ResultTag[NamedTuple.From[T]]): ResultTag[T] =
    val productName = constValue[m.MirroredLabel]
    ProductTag(productName, fields, m)

  inline given [T](using elementType: ResultTag[T]): ResultTag[List[T]] = ResultTag.ListTag(elementType)

// XXX Due to bugs in Scala compiler, you cannot use `using ev: SimpleTypeResultTag[T]` but must use it via ` : SimpleTypeResultTag[T]`.
// XXX the @implicitNotFound therefore cannot be used on the use-site, but must be placed on this trait.
@scala.annotation.implicitNotFound("You can only call contains on queries that return a simple list of values, like Double. You might want to map your result like this: q.map(row => row.price)")
trait SimpleTypeResultTag[T] {}
object SimpleTypeResultTag:
  given SimpleTypeResultTag[scala.Null] = new SimpleTypeResultTag {}
  given SimpleTypeResultTag[Int] = new SimpleTypeResultTag {}
  given SimpleTypeResultTag[String] = new SimpleTypeResultTag {}
  given SimpleTypeResultTag[Boolean] = new SimpleTypeResultTag {}
  given SimpleTypeResultTag[Double] = new SimpleTypeResultTag {}
  given SimpleTypeResultTag[LocalDate] = new SimpleTypeResultTag {}
