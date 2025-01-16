package tyql

import java.time.{LocalDate, LocalDateTime}
import scala.NamedTuple.NamedTuple
import scala.compiletime.{constValue, constValueTuple, summonAll}
import scala.deriving.Mirror

// XXX Do not remove simple types form ResultTag, that would make the driver more annoying.
// XXX This slight code duplication is OK since there are so few simple types.

@annotation.implicitNotFound("Tyql only supports certain types of results. This type is unsupported: ${T}")
enum ResultTag[T]:
  case NullTag extends ResultTag[scala.Null]
  case ByteArrayTag extends ResultTag[Array[Byte]]
  case ByteStreamTag extends ResultTag[() => java.io.InputStream]
  case IntTag extends ResultTag[Int]
  case LongTag extends ResultTag[Long]
  case FloatTag extends ResultTag[Float]
  case DoubleTag extends ResultTag[Double]
  case StringTag extends ResultTag[String]
  case BoolTag extends ResultTag[Boolean]
  case LocalDateTag extends ResultTag[LocalDate]
  case LocalDateTimeTag extends ResultTag[LocalDateTime]
  case NamedTupleTag[N <: Tuple, V <: Tuple](var names: List[String], types: List[ResultTag[?]])
      extends ResultTag[NamedTuple[N, V]]
  case ProductTag[T](productName: String, fields: ResultTag[NamedTuple.From[T]], m: Mirror.ProductOf[T])
      extends ResultTag[T]
  case ListTag[T](elementType: ResultTag[T]) extends ResultTag[List[T]]
  case OptionalTag[T](elementType: ResultTag[T]) extends ResultTag[Option[T]]
  case AnyTag extends ResultTag[Any]

object ResultTag:
  given ResultTag[scala.Null] = ResultTag.NullTag
  given ResultTag[Array[Byte]] = ResultTag.ByteArrayTag
  given ResultTag[() => java.io.InputStream] = ResultTag.ByteStreamTag
  given ResultTag[Int] = ResultTag.IntTag
  given ResultTag[Long] = ResultTag.LongTag
  given ResultTag[String] = ResultTag.StringTag
  given ResultTag[Boolean] = ResultTag.BoolTag
  given ResultTag[Double] = ResultTag.DoubleTag
  given ResultTag[Float] = ResultTag.FloatTag
  given ResultTag[LocalDate] = ResultTag.LocalDateTag
  given ResultTag[LocalDateTime] = ResultTag.LocalDateTimeTag
  given [T](using e: ResultTag[T]): ResultTag[Option[T]] = ResultTag.OptionalTag(e)
  inline given [N <: Tuple, V <: Tuple]: ResultTag[NamedTuple[N, V]] =
    val names = constValueTuple[N]
    val tpes = summonAll[Tuple.Map[V, ResultTag]]
    NamedTupleTag(names.toList.asInstanceOf[List[String]], tpes.toList.asInstanceOf[List[ResultTag[?]]])

  // XXX We don't really need `fields` and could use `m` for everything, but maybe we can share a cached
  // XXX version of `fields`.
  // XXX Alternatively if we don't care about the case class name we could use only `fields`.
  inline given [T](using m: Mirror.ProductOf[T], fields: ResultTag[NamedTuple.From[T]]): ResultTag[T] =
    val productName = constValue[m.MirroredLabel]
    ProductTag(productName, fields, m)

  inline given [T](using elementType: ResultTag[T]): ResultTag[List[T]] = ResultTag.ListTag(elementType)

trait SimpleTypeResultTag[T] {}
object SimpleTypeResultTag:
  given SimpleTypeResultTag[scala.Null] = new SimpleTypeResultTag {}
  given SimpleTypeResultTag[Int] = new SimpleTypeResultTag {}
  given SimpleTypeResultTag[Long] = new SimpleTypeResultTag {}
  given SimpleTypeResultTag[String] = new SimpleTypeResultTag {}
  given SimpleTypeResultTag[Boolean] = new SimpleTypeResultTag {}
  given SimpleTypeResultTag[Double] = new SimpleTypeResultTag {}
  given SimpleTypeResultTag[Float] = new SimpleTypeResultTag {}
  given SimpleTypeResultTag[LocalDate] = new SimpleTypeResultTag {}
  given SimpleTypeResultTag[LocalDateTime] = new SimpleTypeResultTag {}
