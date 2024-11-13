package tyql

import scala.annotation.targetName
import language.experimental.namedTuples
import NamedTuple.{AnyNamedTuple, NamedTuple}
import scala.deriving.*
import scala.compiletime.{erasedValue, summonInline}
import tyql.DialectFeature
import scala.util.NotGiven
import scala.annotation.implicitNotFound

sealed trait ExprShape
class ScalarExpr extends ExprShape
class NonScalarExpr extends ExprShape

// the compiler cannot understand that this meand that combining with NonScalarExpr means identity
// if you do CalculatedShape[unknownShape, NonScalarExpr] it will not understand that this is equal to unknownShape
type CalculatedShape[S1 <: ExprShape, S2 <: ExprShape] <: ExprShape =
  (S1, S2) match
    case (ScalarExpr, ScalarExpr)       => ScalarExpr
    case (ScalarExpr, NonScalarExpr)    => ScalarExpr
    case (NonScalarExpr, ScalarExpr)    => ScalarExpr
    case (NonScalarExpr, NonScalarExpr) => NonScalarExpr

@implicitNotFound(
  "Equality semantics differ between dialects. E.g. in Postgres you can only compare similar types. To express equality or inequality, please import a dialect like this: `import tyql.Dialect.mysql.given`. If you have imported a dialect, then the selected dialect does not support equality between ${T1} and ${T2}."
)
trait CanBeEqualed[T1, T2]

private[tyql] enum CastTarget:
  case CInt, CString, CDouble, CBool, CFloat, CLong

trait LiteralExpression {}

/** The type of expressions in the query language */
trait Expr[Result, Shape <: ExprShape](using val tag: ResultTag[Result]) extends Selectable:
  /** This type is used to support selection with any of the field names defined by Fields.
    */
  type Fields = NamedTuple.Map[NamedTuple.From[Result], [T] =>> Expr[T, Shape]]

  /** A selection of a field name defined by Fields is implemented by `selectDynamic`. The implementation will add a
    * cast to the right Expr type corresponding to the field type.
    */
  def selectDynamic(fieldName: String) = Expr.Select(this, fieldName)

  /** Member methods to implement universal equality on Expr level. */
  def ==[T, S <: ExprShape]
    (other: Expr[T, S])
    (using CanBeEqualed[Result, T])
    : Expr[Boolean, CalculatedShape[Shape, S]] = Expr.Eq[Shape, S](this, other)
  def ===[T, S <: ExprShape]
    (other: Expr[T, S])
    (using CanBeEqualed[Result, T])
    : Expr[Boolean, CalculatedShape[Shape, S]] = Expr.NullSafeEq[Shape, S](this, other)

  def !=[T, Shape2 <: ExprShape]
    (other: Expr[T, Shape2])
    (using CanBeEqualed[Result, T])
    : Expr[Boolean, CalculatedShape[Shape, Shape2]] =
    Expr.Ne[Shape, Shape2](this, other)
  def !==[T, Shape2 <: ExprShape]
    (other: Expr[T, Shape2])
    (using CanBeEqualed[Result, T])
    : Expr[Boolean, CalculatedShape[Shape, Shape2]] =
    Expr.NullSafeNe[Shape, Shape2](this, other)

  // XXX these are ugly, but hard to remove, since we are running in live Scala, the compiler likes to interpret `==` as a native equality and complain
  def ==(other: String): Expr[Boolean, CalculatedShape[Shape, NonScalarExpr]] = Expr.Eq(this, Expr.StringLit(other))
  def ==(other: Int): Expr[Boolean, CalculatedShape[Shape, NonScalarExpr]] = Expr.Eq(this, Expr.IntLit(other))
  def ==(other: Boolean): Expr[Boolean, CalculatedShape[Shape, NonScalarExpr]] = Expr.Eq(this, Expr.BooleanLit(other))
  def ==(other: Double): Expr[Boolean, CalculatedShape[Shape, NonScalarExpr]] = Expr.Eq(this, Expr.DoubleLit(other))
  def !=(other: String): Expr[Boolean, CalculatedShape[Shape, NonScalarExpr]] = Expr.Ne(this, Expr.StringLit(other))
  def !=(other: Int): Expr[Boolean, CalculatedShape[Shape, NonScalarExpr]] = Expr.Ne(this, Expr.IntLit(other))
  def !=(other: Boolean): Expr[Boolean, CalculatedShape[Shape, NonScalarExpr]] = Expr.Ne(this, Expr.BooleanLit(other))
  def !=(other: Double): Expr[Boolean, CalculatedShape[Shape, NonScalarExpr]] = Expr.Ne(this, Expr.DoubleLit(other))

  def isNull[S <: ExprShape]: Expr[Boolean, Shape] = Expr.IsNull(this)
  def nullIf[S <: ExprShape](other: Expr[Result, S]): Expr[Result, CalculatedShape[Shape, S]] = Expr.NullIf(this, other)

  // TODO why do we need these `asInstanceOf`?
  def asInt: Expr[Int, Shape] =
    Expr.Cast(this, CastTarget.CInt)(using ResultTag.IntTag.asInstanceOf[ResultTag[Int]])
  def asLong: Expr[Long, Shape] =
    Expr.Cast(this, CastTarget.CLong)(using ResultTag.LongTag.asInstanceOf[ResultTag[Long]])
  def asString: Expr[String, Shape] = Expr.Cast[Result, String, Shape](this, CastTarget.CString)(using
  ResultTag.StringTag.asInstanceOf[ResultTag[String]])
  def asDouble: Expr[Double, Shape] = Expr.Cast[Result, Double, Shape](this, CastTarget.CDouble)(using
  ResultTag.DoubleTag.asInstanceOf[ResultTag[Double]])
  def asFloat: Expr[Float, Shape] =
    Expr.Cast[Result, Float, Shape](this, CastTarget.CFloat)(using ResultTag.FloatTag.asInstanceOf[ResultTag[Float]])
  def asBoolean: Expr[Boolean, Shape] =
    Expr.Cast[Result, Boolean, Shape](this, CastTarget.CBool)(using ResultTag.BoolTag.asInstanceOf[ResultTag[Boolean]])

  def cases[DestinationT: ResultTag, SV <: ExprShape]
    (
        firstCase: (Expr[Result, Shape] | ElseToken, Expr[DestinationT, SV]),
        restOfCases: (Expr[Result, Shape] | ElseToken, Expr[DestinationT, SV])*
    )
    : Expr[DestinationT, SV] =
    type FromT = Result
    var mainCases: collection.mutable.ArrayBuffer[(Expr[FromT, Shape], Expr[DestinationT, SV])] =
      collection.mutable.ArrayBuffer.empty
    var elseCase: Option[Expr[DestinationT, SV]] = None
    val cases = Seq(firstCase) ++ restOfCases
    for (((condition, value), index) <- cases.zipWithIndex) {
      condition match
        case _: ElseToken =>
          assert(index == cases.size - 1, "The default condition must be last")
          elseCase = Some(value)
        case _: Expr[?, ?] =>
          mainCases += ((condition.asInstanceOf[Expr[FromT, Shape]], value))
    }
    Expr.SimpleCase(this, mainCases.toList, elseCase)

object Expr:
  /** Sample extension methods for individual types */
  extension [S1 <: ExprShape](x: Expr[Int, S1])
    def %[S2 <: ExprShape](y: Expr[Int, S2]): Expr[Int, CalculatedShape[S1, S2]] = Modulo(x, y)

  extension [S1 <: ExprShape](x: Expr[String, S1])
    def <[S2 <: ExprShape](y: Expr[String, S2]): Expr[Boolean, CalculatedShape[S1, S2]] =
      Lt(x, y)
    def <=[S2 <: ExprShape](y: Expr[String, S2]): Expr[Boolean, CalculatedShape[S1, S2]] =
      Lte(x, y)
    def >[S2 <: ExprShape](y: Expr[String, S2]): Expr[Boolean, CalculatedShape[S1, S2]] =
      Gt(x, y)
    def >=[S2 <: ExprShape](y: Expr[String, S2]): Expr[Boolean, CalculatedShape[S1, S2]] =
      Gte(x, y)
    def between[S2 <: ExprShape, S3 <: ExprShape]
      (min: Expr[String, S2], max: Expr[String, S3])
      : Expr[Boolean, CalculatedShape[S1, CalculatedShape[S2, S3]]] =
      Between(x, min, max)

  // DIVISION OPERATORS
  /// XXX generated, this actually cannot be done via defining traits FloatingNumber/IntegralNumber since then Scala compiler will complain about ambiguous overloads
  extension [S1 <: ExprShape](x: Expr[Double, S1])
    @targetName("DivDoubleDouble") def /[S2 <: ExprShape](y: Expr[Double, S2]): Expr[Double, CalculatedShape[S1, S2]] =
      Div(x, y)
    @targetName("DivDoubleFloat") def /[S2 <: ExprShape](y: Expr[Float, S2]): Expr[Double, CalculatedShape[S1, S2]] =
      Div(x, y)
    @targetName("DivDoubleLong") def /[S2 <: ExprShape](y: Expr[Long, S2]): Expr[Double, CalculatedShape[S1, S2]] =
      Div(x, y)
    @targetName("DivDoubleInt") def /[S2 <: ExprShape](y: Expr[Int, S2]): Expr[Double, CalculatedShape[S1, S2]] =
      Div(x, y)
  extension [S1 <: ExprShape](x: Expr[Float, S1])
    @targetName("DivFloatDouble") def /[S2 <: ExprShape](y: Expr[Double, S2]): Expr[Double, CalculatedShape[S1, S2]] =
      Div(x, y)
    @targetName("DivFloatFloat") def /[S2 <: ExprShape](y: Expr[Float, S2]): Expr[Double, CalculatedShape[S1, S2]] =
      Div(x, y)
    @targetName("DivFloatLong") def /[S2 <: ExprShape](y: Expr[Long, S2]): Expr[Double, CalculatedShape[S1, S2]] =
      Div(x, y)
    @targetName("DivFloatInt") def /[S2 <: ExprShape](y: Expr[Int, S2]): Expr[Double, CalculatedShape[S1, S2]] =
      Div(x, y)
  extension [S1 <: ExprShape](x: Expr[Long, S1])
    @targetName("DivLongDouble") def /[S2 <: ExprShape](y: Expr[Double, S2]): Expr[Double, CalculatedShape[S1, S2]] =
      Div(x, y)
    @targetName("DivLongFloat") def /[S2 <: ExprShape](y: Expr[Float, S2]): Expr[Double, CalculatedShape[S1, S2]] =
      Div(x, y)
    @targetName("DivLongLong") def /[S2 <: ExprShape](y: Expr[Long, S2]): Expr[Double, CalculatedShape[S1, S2]] =
      Div(Cast[Long, Double, S1](x, CastTarget.CDouble), y)
    @targetName("DivLongInt") def /[S2 <: ExprShape](y: Expr[Int, S2]): Expr[Double, CalculatedShape[S1, S2]] =
      Div(Cast[Long, Double, S1](x, CastTarget.CDouble), y)
  extension [S1 <: ExprShape](x: Expr[Int, S1])
    @targetName("DivIntDouble") def /[S2 <: ExprShape](y: Expr[Double, S2]): Expr[Double, CalculatedShape[S1, S2]] =
      Div(x, y)
    @targetName("DivIntFloat") def /[S2 <: ExprShape](y: Expr[Float, S2]): Expr[Double, CalculatedShape[S1, S2]] =
      Div(x, y)
    @targetName("DivIntLong") def /[S2 <: ExprShape](y: Expr[Long, S2]): Expr[Double, CalculatedShape[S1, S2]] =
      Div(Cast[Int, Double, S1](x, CastTarget.CDouble), y)
    @targetName("DivIntInt") def /[S2 <: ExprShape](y: Expr[Int, S2]): Expr[Double, CalculatedShape[S1, S2]] =
      Div(Cast[Int, Double, S1](x, CastTarget.CDouble), y)

  extension [T: Numeric, S1 <: ExprShape](x: Expr[T, S1])(using ResultTag[T])
    def <[T2: Numeric, S2 <: ExprShape](y: Expr[T2, S2])(using ResultTag[T2]): Expr[Boolean, CalculatedShape[S1, S2]] =
      Lt(x, y)
    def <=[T2: Numeric, S2 <: ExprShape](y: Expr[T2, S2])(using ResultTag[T2]): Expr[Boolean, CalculatedShape[S1, S2]] =
      Lte(x, y)
    def >[T2: Numeric, S2 <: ExprShape](y: Expr[T2, S2])(using ResultTag[T2]): Expr[Boolean, CalculatedShape[S1, S2]] =
      Gt(x, y)
    def >=[T2: Numeric, S2 <: ExprShape](y: Expr[T2, S2])(using ResultTag[T2]): Expr[Boolean, CalculatedShape[S1, S2]] =
      Gte(x, y)
    def between[S2 <: ExprShape, S3 <: ExprShape]
      (min: Expr[T, S2], max: Expr[T, S3])
      : Expr[Boolean, CalculatedShape[S1, CalculatedShape[S2, S3]]] =
      Between(x, min, max)

    def +[S2 <: ExprShape](y: Expr[T, S2]): Expr[T, CalculatedShape[S1, S2]] = Plus(x, y)
    def -[S2 <: ExprShape](y: Expr[T, S2]): Expr[T, CalculatedShape[S1, S2]] = Minus(x, y)
    def *[S2 <: ExprShape](y: Expr[T, S2]): Expr[T, CalculatedShape[S1, S2]] = Times(x, y)

    def abs: Expr[T, S1] = Abs(x)
    def sqrt: Expr[Double, S1] = Sqrt(x)
    def round: Expr[Int, S1] = Round(x)
    def round[S2 <: ExprShape](precision: Expr[Int, S2]): Expr[Double, CalculatedShape[S1, S2]] =
      RoundWithPrecision(x, precision)
    def ceil: Expr[Int, S1] = Ceil(x)
    def floor: Expr[Int, S1] = Floor(x)
    def power[S2 <: ExprShape](y: Expr[Double, S2]): Expr[Double, CalculatedShape[S1, S2]] = Power(x, y)
    def sign: Expr[Int, S1] = Sign(x)
    def ln: Expr[Double, S1] = LogNatural(x)
    def log(base: Expr[T, S1]): Expr[Double, CalculatedShape[S1, S1]] = Log(base, x)
    def log10: Expr[Double, S1] = Log(IntLit(10), x).asInstanceOf[Expr[Double, S1]] // TODO cast?
    def log2: Expr[Double, S1] = Log(IntLit(2), x).asInstanceOf[Expr[Double, S1]] // TODO cast?

  def exp[T: Numeric, S <: ExprShape](x: Expr[T, S])(using ResultTag[T]): Expr[Double, S] = Exp(x)
  def sin[T: Numeric, S <: ExprShape](x: Expr[T, S])(using ResultTag[T]): Expr[Double, S] = Sin(x)
  def cos[T: Numeric, S <: ExprShape](x: Expr[T, S])(using ResultTag[T]): Expr[Double, S] = Cos(x)
  def tan[T: Numeric, S <: ExprShape](x: Expr[T, S])(using ResultTag[T]): Expr[Double, S] = Tan(x)
  def asin[T: Numeric, S <: ExprShape](x: Expr[T, S])(using ResultTag[T]): Expr[Double, S] = Asin(x)
  def acos[T: Numeric, S <: ExprShape](x: Expr[T, S])(using ResultTag[T]): Expr[Double, S] = Acos(x)
  def atan[T: Numeric, S <: ExprShape](x: Expr[T, S])(using ResultTag[T]): Expr[Double, S] = Atan(x)

  extension [S1 <: ExprShape](x: Expr[Boolean, S1])
    def &&[S2 <: ExprShape](y: Expr[Boolean, S2]): Expr[Boolean, CalculatedShape[S1, S2]] = And(x, y)
    def ||[S2 <: ExprShape](y: Expr[Boolean, S2]): Expr[Boolean, CalculatedShape[S1, S2]] = Or(x, y)
    def unary_! = Not(x)
    def ^(y: Expr[Boolean, S1]): Expr[Boolean, S1] = Xor(x, y)

  extension [S1 <: ExprShape, T]
    (x: Expr[Option[T], S1])
    (using
        ev0: ResultTag[T],
        @implicitNotFound(
          "Only simple types like Double can be used inside Option. This is not a simpel type: ${T}"
        ) ev1: SimpleTypeResultTag[T]
    )
    def isEmpty: Expr[Boolean, S1] = Expr.IsNull(x)
    def isDefined: Expr[Boolean, S1] = Not(Expr.IsNull(x))
    def get: Expr[T, S1] = x.asInstanceOf[Expr[T, S1]] // TODO should this error silently?
    def getOrElse(default: Expr[T, S1]): Expr[T, S1] = coalesce(x.asInstanceOf[Expr[T, S1]], default)
    /* ABSTRACTION: To abstract over expressions (not relations) in the DSL, the DSL needs some kind of abstraction/application operation.
      Option 1: (already supported) use host-level abstraction e.g. define a lambda.
      Option 2: Use a macro to do substitution, but then lose the macro-free claim.
      Option 3: You could define an expression-level substitution, but since the case classes in this file use type parameters
                constrained by e.g. Numeric, it would be very annoying to write a type-safe substitution that replaces the expression reference
                by another expression in some larger expression.
      Because we do not want to use macros and the Option.map is expected to have little code, we pick the lambda.
     */
    def map[U: SimpleTypeResultTag]
      (f: Expr[T, NonScalarExpr] => Expr[U, NonScalarExpr])
      (using ResultTag[U])
      : Expr[Option[U], S1] =
      OptionMap(x, f)

    // TODO somehow use options in aggregations

  extension [S1 <: ExprShape](x: Expr[Array[Byte], S1])
    @targetName("byteLengthForArrayByte")
    def byteLength: Expr[Int, S1] =
      Expr.ByteByteLength(x.asInstanceOf[Expr[(Array[Byte] | Function0[java.io.InputStream]), S1]])
  extension [S1 <: ExprShape](x: Expr[() => java.io.InputStream, S1])
    @targetName("byteLengthForjavaioInputStream")
    def byteLength: Expr[Int, S1] =
      Expr.ByteByteLength(x.asInstanceOf[Expr[(Array[Byte] | Function0[java.io.InputStream]), S1]])

  extension [S1 <: ExprShape](x: Expr[String, S1])
    def toLowerCase: Expr[String, S1] = Expr.Lower(x)

    /** Convert the string to upper case via the UPPER SQL function, which will depend on
      *   - the collation of the input string,
      *   - the backend database,
      *   - and the database-specific collation settings.
      *
      * @return
      *   Expr[String, S] where S is the same ExprShape as the input.
      */
    def toUpperCase: Expr[String, S1] = Expr.Upper(x)
    def charLength: Expr[Int, S1] = Expr.StringCharLength(x)
    def length: Expr[Int, S1] = charLength
    def byteLength: Expr[Int, S1] = Expr.StringByteLength(x)
    def stripLeading: Expr[String, S1] = Expr.LTrim(x) // Java naming
    def stripTrailing: Expr[String, S1] = Expr.RTrim(x) // Java naming
    def strip: Expr[String, S1] = Expr.Trim(x) // Java naming
    def ltrim: Expr[String, S1] = Expr.LTrim(x) // SQL naming
    def rtrim: Expr[String, S1] = Expr.RTrim(x) // SQL naming
    def trim: Expr[String, S1] = Expr.Trim(x) // SQL naming
    def replace[S2 <: ExprShape](from: Expr[String, S2], to: Expr[String, S2]): Expr[String, CalculatedShape[S1, S2]] =
      Expr.StrReplace(x, from, to)
    // TODO maybe add assertions that len should be >= 0 and from >= 1 if we know them?
    // SQL semantics (1-based indexing, start+length)
    def substr[S2 <: ExprShape](from: Expr[Int, S2], len: Expr[Int, S2] = null): Expr[String, CalculatedShape[S1, S2]] =
      Expr.Substring(x, from, Option.fromNullable(len))
    // Java semantics (0-based indexing, start+afterLast)
    def substring[S2 <: ExprShape]
      (start: Expr[Int, S2], afterLast: Expr[Int, S2] = null)
      : Expr[String, CalculatedShape[S1, S2]] =
      if afterLast != null then
        substr(
          Expr.Plus(Expr.IntLit(1), start).asInstanceOf[Expr[Int, S2]],
          Expr.Minus(afterLast, start).asInstanceOf[Expr[Int, S2]]
        ) // XXX how to avoid this cast
      else
        substr(Expr.Plus(Expr.IntLit(1), start).asInstanceOf[Expr[Int, S2]], null) // XXX how to avoid this cast
    def like[S2 <: ExprShape](pattern: Expr[String, S2]): Expr[Boolean, CalculatedShape[S1, S2]] =
      Expr.StrLike(x, pattern)
    def `+`[S2 <: ExprShape](y: Expr[String, S2]): Expr[String, CalculatedShape[S1, S2]] = Expr.StrConcat(x, Seq(y))
    def reverse(using DialectFeature.ReversibleStrings): Expr[String, S1] = Expr.StrReverse(x)
    def repeat[S2 <: ExprShape](n: Expr[Int, S2]): Expr[String, CalculatedShape[S1, S2]] = Expr.StrRepeat(x, n)
    def lpad[S2 <: ExprShape](len: Expr[Int, S2], pad: Expr[String, S2]): Expr[String, CalculatedShape[S1, S2]] =
      Expr.StrLPad(x, len, pad)
    def rpad[S2 <: ExprShape](len: Expr[Int, S2], pad: Expr[String, S2]): Expr[String, CalculatedShape[S1, S2]] =
      Expr.StrRPad(x, len, pad)
    def findPosition[S2 <: ExprShape](substr: Expr[String, S2]): Expr[Int, CalculatedShape[S1, S2]] =
      Expr.StrPositionIn(substr, x)

  def coalesce[T, S1 <: ExprShape](x: Expr[T, S1], y: Expr[T, S1], xs: Expr[T, S1]*)(using ResultTag[T]): Expr[T, S1] =
    Coalesce(x, y, xs)
  def nullIf[T, S1 <: ExprShape, S2 <: ExprShape]
    (x: Expr[T, S1], y: Expr[T, S2])
    (using ResultTag[T])
    : Expr[T, CalculatedShape[S1, S2]] = NullIf(x, y)

  def concat[S <: ExprShape](strs: Seq[Expr[String, S]]): Expr[String, S] =
    assert(strs.nonEmpty, "concat requires at least one argument")
    StrConcatUniform(strs.head, strs.tail)
  // TODO XXX this cannot be named concat since then Scala will never resolve it, it will always try for the first version without the sep parameter.
  def concatWith[S <: ExprShape, SS <: ExprShape]
    (strs: Seq[Expr[String, S]], sep: Expr[String, SS])
    : Expr[String, CalculatedShape[S, SS]] =
    assert(strs.nonEmpty, "concatWith requires at least one argument")
    StrConcatSeparator(sep, strs.head, strs.tail)

  extension [A](x: Expr[List[A], NonScalarExpr])(using ResultTag[A], ResultTag[List[A]])
    def prepend(elem: Expr[A, NonScalarExpr]): Expr[List[A], NonScalarExpr] = ListPrepend(elem, x)
    def append(elem: Expr[A, NonScalarExpr]): Expr[List[A], NonScalarExpr] = ListAppend(x, elem)
    // XXX Due to Scala overloading bugs, there can be no two extensions methods named `contains` with similar arguments.
    // XXX Because the list one is less used, this one is called `containsElement` instead.
    def containsElement(elem: Expr[A, NonScalarExpr]): Expr[Boolean, NonScalarExpr] = ListContains(x, elem)
    def length: Expr[Int, NonScalarExpr] = ListLength(x)
    def ++(other: Expr[List[A], NonScalarExpr]): Expr[List[A], NonScalarExpr] = ListConcat(x, other)
    def apply(i: Expr[Int, NonScalarExpr]): Expr[A, NonScalarExpr] = ListGet(x, Expr.Plus(i, Expr.IntLit(1)))

  // Aggregations
  def sum[T : ResultTag : Numeric](x: Expr[T, ?]): AggregationExpr[T] = AggregationExpr.Sum(x)
  def avg[T : ResultTag : Numeric](x: Expr[T, ?]): AggregationExpr[T] = AggregationExpr.Avg(x)
  def max[T : ResultTag : Numeric](x: Expr[T, ?]): AggregationExpr[T] = AggregationExpr.Max(x)
  def min[T : ResultTag : Numeric](x: Expr[T, ?]): AggregationExpr[T] = AggregationExpr.Min(x)
  @targetName("maxAggForStrings")
  def max(x: Expr[String, ?]): AggregationExpr[String] = AggregationExpr.Max(x)
  @targetName("minAggForStrings")
  def min(x: Expr[String, ?]): AggregationExpr[String] = AggregationExpr.Min(x)
  def count[T: ResultTag](x: Expr[T, ?]): AggregationExpr[Int] = AggregationExpr.Count(x)

  // date extractions
  extension (d: Expr[java.time.LocalDate, NonScalarExpr])
    def year = DateYear(d)
    def month = DateMonth(d)
    def day = DateDay(d)
  extension (d: Expr[java.time.LocalDateTime, NonScalarExpr])
    def year = TimestampYear(d)
    def month = TimestampMonth(d)
    def day = TimestampDay(d)
    def hour = TimestampHours(d)
    def minute = TimestampMinutes(d)
    def second = TimestampSeconds(d)

  // Window function expressions
  def rowNumber: ExprInWindowPosition[Int] = RowNumber()
  def rank: ExprInWindowPosition[Int] = Rank()
  def denseRank: ExprInWindowPosition[Int] = DenseRank()
  def ntile(n: Int): ExprInWindowPosition[Int] = NTile(n)
  def lag[R, S <: ExprShape]
    (e: Expr[R, S], offset: Int, default: Expr[R, S])
    (using ResultTag[R])
    : ExprInWindowPosition[R] = Lag(e, Some(offset), Some(default))
  def lag[R, S <: ExprShape](e: Expr[R, S], offset: Int)(using ResultTag[R]): ExprInWindowPosition[R] =
    Lag(e, Some(offset), None)
  def lag[R, S <: ExprShape](e: Expr[R, S])(using ResultTag[R]): ExprInWindowPosition[R] = Lag(e, None, None)
  def lead[R, S <: ExprShape]
    (e: Expr[R, S], offset: Int, default: Expr[R, S])
    (using ResultTag[R])
    : ExprInWindowPosition[R] = Lead(e, Some(offset), Some(default))
  def lead[R, S <: ExprShape](e: Expr[R, S], offset: Int)(using ResultTag[R]): ExprInWindowPosition[R] =
    Lead(e, Some(offset), None)
  def lead[R, S <: ExprShape](e: Expr[R, S])(using ResultTag[R]): ExprInWindowPosition[R] = Lead(e, None, None)
  def firstValue[R, S <: ExprShape](e: Expr[R, S])(using ResultTag[R]): ExprInWindowPosition[R] = FirstValue(e)
  def lastValue[R, S <: ExprShape](e: Expr[R, S])(using ResultTag[R]): ExprInWindowPosition[R] = LastValue(e)
  def nthValue[R, S <: ExprShape](e: Expr[R, S], n: Int)(using ResultTag[R]): ExprInWindowPosition[R] = NthValue(e, n)

  // Note: All field names of constructors in the query language are prefixed with `$`
  // so that we don't accidentally pick a field name of a constructor class where we want
  // a name in the domain model instead.

  case class VariableInput[T]($thunk: () => T)(using t: ResultTag[T], s: SimpleTypeResultTag[T])
      extends Expr[T, NonScalarExpr] {
    def evaluateToLiteral(): Expr[T, NonScalarExpr] = {
      val got = $thunk()
      if got == null then return tyql.Null
      t match
        case ResultTag.NullTag          => tyql.Null
        case ResultTag.IntTag           => IntLit(got.asInstanceOf[Int])
        case ResultTag.LongTag          => LongLit(got.asInstanceOf[Long])
        case ResultTag.DoubleTag        => DoubleLit(got.asInstanceOf[Double])
        case ResultTag.FloatTag         => FloatLit(got.asInstanceOf[Float])
        case ResultTag.StringTag        => StringLit(got.asInstanceOf[String])
        case ResultTag.BoolTag          => BooleanLit(got.asInstanceOf[Boolean])
        case ResultTag.LocalDateTag     => LocalDateLit(got.asInstanceOf[java.time.LocalDate])
        case ResultTag.LocalDateTimeTag => LocalDateTimeLit(got.asInstanceOf[java.time.LocalDateTime])
        case _                          => assert(false, "Unexpected type " + t)
    }
  }

  // These case classes could technically contain e.g. 1 < 'a', but there are no user-exposes functions to create such expressions
  type IsString[T] = T =:= String
  type Comparable[T] = Numeric[T] | IsString[T]
  case class Lt[T1: Comparable, T2: Comparable, S1 <: ExprShape, S2 <: ExprShape]
    ($x: Expr[T1, S1], $y: Expr[T2, S2])
    (using ResultTag[T1], ResultTag[T2]) extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class Lte[T1: Comparable, T2: Comparable, S1 <: ExprShape, S2 <: ExprShape]
    ($x: Expr[T1, S1], $y: Expr[T2, S2])
    (using ResultTag[T1], ResultTag[T2]) extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class Gt[T1: Comparable, T2: Comparable, S1 <: ExprShape, S2 <: ExprShape]
    ($x: Expr[T1, S1], $y: Expr[T2, S2])
    (using ResultTag[T1], ResultTag[T2]) extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class Gte[T1: Comparable, T2: Comparable, S1 <: ExprShape, S2 <: ExprShape]
    ($x: Expr[T1, S1], $y: Expr[T2, S2])
    (using ResultTag[T1], ResultTag[T2]) extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class Between[T: Comparable, S1 <: ExprShape, S2 <: ExprShape, S3 <: ExprShape]
    ($x: Expr[T, S1], $min: Expr[T, S2], $max: Expr[T, S3])
    (using ResultTag[T]) extends Expr[Boolean, CalculatedShape[S1, CalculatedShape[S2, S3]]]

  case class FunctionCall0NoParentheses[R](name: String)(using ResultTag[R])
      extends Expr[R, NonScalarExpr]
  // XXX maybe remove these FunctionCall_, for now they only exist to create fake AST trees for debug purposes...
  case class FunctionCall0[R](name: String)(using ResultTag[R])
      extends Expr[R, NonScalarExpr]
  case class FunctionCall1[A1, R, S1 <: ExprShape](name: String, $a1: Expr[A1, S1])(using ResultTag[R])
      extends Expr[R, S1]
  case class FunctionCall2[A1, A2, R, S1 <: ExprShape, S2 <: ExprShape]
    (name: String, $a1: Expr[A1, S1], $a2: Expr[A2, S2])
    (using ResultTag[R]) extends Expr[R, CalculatedShape[S1, S2]]

  case class Plus[S1 <: ExprShape, S2 <: ExprShape, T: Numeric]($x: Expr[T, S1], $y: Expr[T, S2])(using ResultTag[T])
      extends Expr[T, CalculatedShape[S1, S2]]
  case class Minus[S1 <: ExprShape, S2 <: ExprShape, T: Numeric]($x: Expr[T, S1], $y: Expr[T, S2])(using ResultTag[T])
      extends Expr[T, CalculatedShape[S1, S2]]
  case class Times[S1 <: ExprShape, S2 <: ExprShape, T: Numeric]($x: Expr[T, S1], $y: Expr[T, S2])(using ResultTag[T])
      extends Expr[T, CalculatedShape[S1, S2]]
  case class Div[S1 <: ExprShape, S2 <: ExprShape, T1, T2]
    ($x: Expr[T1, S1], $y: Expr[T2, S2])
    (using ResultTag[T1], ResultTag[T2])
      extends Expr[Double, CalculatedShape[S1, S2]]

  case class And[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[Boolean, S1], $y: Expr[Boolean, S2])
      extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class Or[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[Boolean, S1], $y: Expr[Boolean, S2])
      extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class Not[S1 <: ExprShape]($x: Expr[Boolean, S1]) extends Expr[Boolean, S1]
  case class Xor[S1 <: ExprShape]($x: Expr[Boolean, S1], $y: Expr[Boolean, S1]) extends Expr[Boolean, S1]

  case class ByteByteLength[S <: ExprShape]($x: Expr[(Array[Byte] | Function0[java.io.InputStream]), S])
      extends Expr[Int, S]

  case class Upper[S <: ExprShape]($x: Expr[String, S]) extends Expr[String, S]
  case class Lower[S <: ExprShape]($x: Expr[String, S]) extends Expr[String, S]
  case class StringCharLength[S <: ExprShape]($x: Expr[String, S]) extends Expr[Int, S]
  case class StringByteLength[S <: ExprShape]($x: Expr[String, S]) extends Expr[Int, S]
  case class Trim[S <: ExprShape]($x: Expr[String, S]) extends Expr[String, S]
  case class LTrim[S <: ExprShape]($x: Expr[String, S]) extends Expr[String, S]
  case class RTrim[S <: ExprShape]($x: Expr[String, S]) extends Expr[String, S]
  case class StrReplace[S <: ExprShape, S2 <: ExprShape]
    ($s: Expr[String, S], $from: Expr[String, S2], $to: Expr[String, S2]) extends Expr[String, CalculatedShape[S, S2]]
  case class Substring[S <: ExprShape, S2 <: ExprShape]
    ($s: Expr[String, S], $from: Expr[Int, S2], $len: Option[Expr[Int, S2]])
      extends Expr[String, CalculatedShape[S, S2]]
  case class StrLike[S <: ExprShape, S2 <: ExprShape]($s: Expr[String, S], $pattern: Expr[String, S2])
      extends Expr[Boolean, CalculatedShape[S, S2]] // NonScalar like StringLit
  case class StrConcat[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[String, S1], $xs: Seq[Expr[String, S2]])
      extends Expr[String, CalculatedShape[S1, S2]] // First one has a different shape so you can use it as an opertor between two arguments that have different shapes
  case class StrConcatUniform[S1 <: ExprShape]($x: Expr[String, S1], $xs: Seq[Expr[String, S1]])
      extends Expr[String, S1]
  case class StrConcatSeparator[S1 <: ExprShape, S3 <: ExprShape]
    ($sep: Expr[String, S3], $x: Expr[String, S1], $xs: Seq[Expr[String, S1]])
      extends Expr[String, CalculatedShape[S1, S3]]
  case class StrReverse[S <: ExprShape]($x: Expr[String, S]) extends Expr[String, S]
  case class StrRepeat[S1 <: ExprShape, S2 <: ExprShape]($s: Expr[String, S1], $n: Expr[Int, S2])
      extends Expr[String, CalculatedShape[S1, S2]]
  case class StrLPad[S1 <: ExprShape, S2 <: ExprShape]
    ($s: Expr[String, S1], $len: Expr[Int, S2], $pad: Expr[String, S2]) extends Expr[String, CalculatedShape[S1, S2]]
  case class StrRPad[S1 <: ExprShape, S2 <: ExprShape]
    ($s: Expr[String, S1], $len: Expr[Int, S2], $pad: Expr[String, S2]) extends Expr[String, CalculatedShape[S1, S2]]
  case class StrPositionIn[S1 <: ExprShape, S2 <: ExprShape]($substr: Expr[String, S2], $string: Expr[String, S1])
      extends Expr[Int, CalculatedShape[S1, S2]]

  case class Modulo[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[Int, S1], $y: Expr[Int, S2])
      extends Expr[Int, CalculatedShape[S1, S2]]
  // TODO actually, it's unclear for now what types should be here, the input to ROUND() in most DBs can be any numeric and the ouput is usually of the same type as the input
  case class Round[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Int, S1]
  case class RoundWithPrecision[S1 <: ExprShape, S2 <: ExprShape, T: Numeric]
    ($x: Expr[T, S1], $precision: Expr[Int, S2])
    (using ResultTag[T]) extends Expr[Double, CalculatedShape[S1, S2]]
  case class Ceil[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Int, S1]
  case class Floor[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Int, S1]
  case class Power[S1 <: ExprShape, S2 <: ExprShape, T1: Numeric, T2: Numeric]
    ($x: Expr[T1, S1], $y: Expr[T2, S2])
    (using ResultTag[T1], ResultTag[T2]) extends Expr[Double, CalculatedShape[S1, S2]]
  case class Sqrt[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Double, S1]
  case class Abs[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[T, S1]
  case class Sign[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Int, S1]
  case class LogNatural[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Double, S1]
  case class Log[S1 <: ExprShape, S2 <: ExprShape, T1: Numeric, T2: Numeric]
    ($base: Expr[T1, S1], $x: Expr[T2, S2])
    (using ResultTag[T1], ResultTag[T2]) extends Expr[Double, CalculatedShape[S1, S2]]
  case class Exp[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Double, S1]
  case class Sin[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Double, S1]
  case class Cos[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Double, S1]
  case class Tan[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Double, S1]
  case class Asin[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Double, S1]
  case class Acos[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Double, S1]
  case class Atan[S1 <: ExprShape, T: Numeric]($x: Expr[T, S1])(using ResultTag[T]) extends Expr[Double, S1]

  case class RandomUUID() extends Expr[String, NonScalarExpr]
  case class RandomFloat() extends Expr[Double, NonScalarExpr]
  case class RandomInt[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[Int, S1], $y: Expr[Int, S2])
      extends Expr[Int, CalculatedShape[S1, S2]]

  case class DateYear[S <: ExprShape]($x: Expr[java.time.LocalDate, S]) extends Expr[Int, S]
  case class DateMonth[S <: ExprShape]($x: Expr[java.time.LocalDate, S]) extends Expr[Int, S]
  case class DateDay[S <: ExprShape]($x: Expr[java.time.LocalDate, S]) extends Expr[Int, S]
  case class TimestampYear[S <: ExprShape]($x: Expr[java.time.LocalDateTime, S]) extends Expr[Int, S]
  case class TimestampMonth[S <: ExprShape]($x: Expr[java.time.LocalDateTime, S]) extends Expr[Int, S]
  case class TimestampDay[S <: ExprShape]($x: Expr[java.time.LocalDateTime, S]) extends Expr[Int, S]
  case class TimestampHours[S <: ExprShape]($x: Expr[java.time.LocalDateTime, S]) extends Expr[Int, S]
  case class TimestampMinutes[S <: ExprShape]($x: Expr[java.time.LocalDateTime, S]) extends Expr[Int, S]
  case class TimestampSeconds[S <: ExprShape]($x: Expr[java.time.LocalDateTime, S]) extends Expr[Int, S]

  case class ListExpr[A]
    ($elements: List[Expr[A, NonScalarExpr]])
    (using
        t: ResultTag[List[A]],
        @implicitNotFound(
          "Lists/Arrays in SQL can only be of simple types. This is not a simple type: ${A}"
        ) st: SimpleTypeResultTag[A]
    ) extends Expr[List[A], NonScalarExpr]
  extension [A, E <: Expr[A, NonScalarExpr]](x: List[E])
    def toExpr(using ResultTag[List[A]], SimpleTypeResultTag[A]): ListExpr[A] = ListExpr(x)
  given [A : SimpleTypeResultTag : ResultTag]: Conversion[List[A], Expr[List[A], NonScalarExpr]] =
    lst => ListExpr(lst.map(x => dynamicLit(x)))

  case class ListPrepend[A]($x: Expr[A, NonScalarExpr], $list: Expr[List[A], NonScalarExpr])(using ResultTag[List[A]])
      extends Expr[List[A], NonScalarExpr]
  case class ListAppend[A]($list: Expr[List[A], NonScalarExpr], $x: Expr[A, NonScalarExpr])(using ResultTag[List[A]])
      extends Expr[List[A], NonScalarExpr]
  case class ListContains[A]($list: Expr[List[A], NonScalarExpr], $x: Expr[A, NonScalarExpr])(using ResultTag[Boolean])
      extends Expr[Boolean, NonScalarExpr]
  case class ListLength[A]($list: Expr[List[A], NonScalarExpr])(using ResultTag[Int]) extends Expr[Int, NonScalarExpr]
  case class ListGet[A]($list: Expr[List[A], NonScalarExpr], $i: Expr[Int, NonScalarExpr])(using ResultTag[A])
      extends Expr[A, NonScalarExpr]
  case class ListConcat[A]
    ($xs: Expr[List[A], NonScalarExpr], $ys: Expr[List[A], NonScalarExpr])
    (using ResultTag[List[A]])
      extends Expr[List[A], NonScalarExpr]

  // So far Select is weakly typed, so `selectDynamic` is easy to implement.
  // TODO: Make it strongly typed like the other cases
  case class Select[A: ResultTag]($x: Expr[A, ?], $name: String)
      extends Expr[
        A,
        NonScalarExpr
      ]

  case class Concat[A <: AnyNamedTuple, B <: AnyNamedTuple, S1 <: ExprShape, S2 <: ExprShape]
    ($x: Expr[A, S1], $y: Expr[B, S2])
    (using ResultTag[NamedTuple.Concat[A, B]]) extends Expr[NamedTuple.Concat[A, B], CalculatedShape[S1, S2]]

  case class Project[A <: AnyNamedTuple]($a: A)(using ResultTag[NamedTuple.Map[A, StripExpr]])
      extends Expr[NamedTuple.Map[A, StripExpr], NonScalarExpr]

  type StripExpr[E] = E match
    case Expr[b, s]         => b
    case Expr[b, ?]         => b
    case AggregationExpr[b] => b
    case _ =>
      E // XXX this branch is used for the added flexibility of using literal directly in the insertions and updates

  // Also weakly typed in the arguments since these two classes model universal equality */
  case class Eq[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[?, S1], $y: Expr[?, S2])
      extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class Ne[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[?, S1], $y: Expr[?, S2])
      extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class NullSafeEq[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[?, S1], $y: Expr[?, S2])
      extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class NullSafeNe[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[?, S1], $y: Expr[?, S2])
      extends Expr[Boolean, CalculatedShape[S1, S2]]

  // Expressions resulting from queries
  // Cannot use Contains with an aggregation
  case class Contains[A]($query: Query[A, ?], $expr: Expr[A, NonScalarExpr]) extends Expr[Boolean, NonScalarExpr]
  case class IsEmpty[A]($this: Query[A, ?]) extends Expr[Boolean, NonScalarExpr]
  case class NonEmpty[A]($this: Query[A, ?]) extends Expr[Boolean, NonScalarExpr]

  /** References are placeholders for parameters */
  // XXX currently queries share this counter, which might result in larger numbers over time, but should not be dangerous since these are longs
  private var refCount = 0L
  private var exprRefCount = 0L

  // References to relations
  case class Ref[A: ResultTag, S <: ExprShape](idx: Int = -1) extends Expr[A, S]:
    private val $id = refCount
    refCount += 1
    val idxStr = if idx == -1 then "" else s"_$idx"
    def stringRef() = s"ref${$id}$idxStr"
    override def toString: String = s"Ref[${stringRef()}]$idxStr"

  /** The internal representation of a function `A => B` Query languages are usually first-order, so Fun is not an Expr
    */
  case class Fun[A, B, S <: ExprShape]($param: Ref[A, S], $body: B)

  // TODO aren't these types too restrictive?
  case class SearchedCase[T, SC <: ExprShape, SV <: ExprShape]
    ($cases: List[(Expr[Boolean, SC], Expr[T, SV])], $else: Option[Expr[T, SV]])
    (using ResultTag[T]) extends Expr[T, SV]
  case class SimpleCase[TE, TR, SE <: ExprShape, SR <: ExprShape]
    ($expr: Expr[TE, SE], $cases: List[(Expr[TE, SE], Expr[TR, SR])], $else: Option[Expr[TR, SR]])
    (using ResultTag[TE], ResultTag[TR]) extends Expr[TR, SR]

  case class OptionMap[A, B, S <: ExprShape]
    ($x: Expr[Option[A], S], $f: Expr[A, NonScalarExpr] => Expr[B, NonScalarExpr])
    (using ResultTag[A], ResultTag[B], SimpleTypeResultTag[A], SimpleTypeResultTag[B]) extends Expr[Option[B], S]

  case class Cast[A, B, S <: ExprShape]($x: Expr[A, S], resultType: CastTarget)(using ResultTag[B]) extends Expr[B, S]

  final case class NullLit[A]()(using ResultTag[A]) extends Expr[A, NonScalarExpr] with LiteralExpression
  case class IsNull[A, S <: ExprShape]($x: Expr[A, S]) extends Expr[Boolean, S]
  case class Coalesce[A, S1 <: ExprShape]($x1: Expr[A, S1], $x2: Expr[A, S1], $xs: Seq[Expr[A, S1]])(using ResultTag[A])
      extends Expr[A, S1]
  case class NullIf[A, S1 <: ExprShape, S2 <: ExprShape]($x: Expr[A, S1], $y: Expr[A, S2])(using ResultTag[A])
      extends Expr[A, CalculatedShape[S1, S2]]

  final case class LocalDateLit($value: java.time.LocalDate) extends Expr[java.time.LocalDate, NonScalarExpr]
      with LiteralExpression
  given Conversion[java.time.LocalDate, LocalDateLit] = LocalDateLit(_)
  case class LocalDateTimeLit($value: java.time.LocalDateTime) extends Expr[java.time.LocalDateTime, NonScalarExpr]
      with LiteralExpression
  given Conversion[java.time.LocalDateTime, LocalDateTimeLit] = LocalDateTimeLit(_)

  /** Literals are type-specific, tailored to the types that the DB supports */
  final case class BytesLit($value: Array[Byte]) extends Expr[Array[Byte], NonScalarExpr]
  final case class ByteStreamLit($value: () => java.io.InputStream)
      extends Expr[() => java.io.InputStream, NonScalarExpr]

  final case class IntLit($value: Int) extends Expr[Int, NonScalarExpr] with LiteralExpression
  final case class LongLit($value: Long) extends Expr[Long, NonScalarExpr] with LiteralExpression

  /** Scala values can be lifted into literals by conversions */
  given Conversion[Int, IntLit] = IntLit(_)
  given Conversion[Long, LongLit] = LongLit(_)

  final case class StringLit($value: String) extends Expr[String, NonScalarExpr] with LiteralExpression
  given Conversion[String, StringLit] = StringLit(_)

  final case class DoubleLit($value: Double) extends Expr[Double, NonScalarExpr] with LiteralExpression
  given Conversion[Double, DoubleLit] = DoubleLit(_)

  final case class FloatLit($value: Float) extends Expr[Float, NonScalarExpr] with LiteralExpression
  given Conversion[Float, FloatLit] = FloatLit(_)

  final case class BooleanLit($value: Boolean) extends Expr[Boolean, NonScalarExpr] with LiteralExpression
  // given Conversion[Boolean, BooleanLit] = BooleanLit(_) // XXX this one breaks everything by inlining complex expressions as FALSE everywhere

  def randomFloat(): Expr[Double, NonScalarExpr] = RandomFloat()
  def randomUUID(using r: DialectFeature.RandomUUID)(): Expr[String, NonScalarExpr] = RandomUUID()
  def randomInt[S1 <: ExprShape, S2 <: ExprShape]
    (a: Expr[Int, S1], b: Expr[Int, S2])
    (using r: DialectFeature.RandomIntegerInInclusiveRange)
    : Expr[Int, CalculatedShape[S1, S2]] =
    // TODO maybe add a check for (a <= b) if we know both components at generation time?
    RandomInt(a, b)

  /** Should be able to rely on the implicit conversions, but not always. One approach is to overload, another is to
    * provide a user-facing toExpr function.
    */
//  def toExpr[T](t: T): Expr[T, NonScalarExpr] = t match
//    case t:Int => IntLit(t)
//    case t:Double => DoubleLit(t)
//    case t:String => StringLit(t)
//    case t:Boolean => BooleanLit(t)

  type Pred[A, S <: ExprShape] = Fun[A, Expr[Boolean, S], S]

  type IsTupleOfExpr[A <: AnyNamedTuple] = Tuple.Union[NamedTuple.DropNames[A]] <:< Expr[?, NonScalarExpr]

  /** Explicit conversion from (name_1: Expr[T_1], ..., name_n: Expr[T_n]) to Expr[(name_1: T_1, ..., name_n: T_n)]
    */
  extension [A <: AnyNamedTuple : IsTupleOfExpr](x: A)
    def toRow(using ResultTag[NamedTuple.Map[A, StripExpr]]): Project[A] = Project(x)

// TODO: use NamedTuple.from to convert case classes to named tuples before using concat
  extension [A <: AnyNamedTuple, S <: ExprShape](x: Expr[A, S])
    def concat[B <: AnyNamedTuple, S2 <: ExprShape]
      (other: Expr[B, S2])
      (using ResultTag[NamedTuple.Concat[A, B]])
      : Expr[NamedTuple.Concat[A, B], CalculatedShape[S, S2]] = Concat(x, other)

  /** Same as _.toRow, as an implicit conversion */
//  given [A <: AnyNamedTuple : IsTupleOfExpr](using ResultTag[NamedTuple.Map[A, StripExpr]]): Conversion[A, Expr.Project[A]] = Expr.Project(_)

end Expr

// TODO aren't these types too restrictive?
def cases[T: ResultTag, SC <: ExprShape, SV <: ExprShape]
  (
      firstCase: (Expr[Boolean, SC] | true | ElseToken, Expr[T, SV]),
      restOfCases: (Expr[Boolean, SC] | true | ElseToken, Expr[T, SV])*
  )
  : Expr[T, SV] =
  var mainCases: collection.mutable.ArrayBuffer[(Expr[Boolean, SC], Expr[T, SV])] =
    collection.mutable.ArrayBuffer.empty
  var elseCase: Option[Expr[T, SV]] = None
  val cases = Seq(firstCase) ++ restOfCases
  for (((condition, value), index) <- cases.zipWithIndex) {
    condition match
      case _: ElseToken =>
        assert(index == cases.size - 1, "The default condition must be last")
        elseCase = Some(value)
      case true =>
        assert(index == cases.size - 1, "The default condition must be last")
        elseCase = Some(value)
      case false => assert(false, "what do you mean, false?")
      case _: Expr[?, ?] =>
        mainCases += ((condition.asInstanceOf[Expr[Boolean, SC]], value))
  }
  Expr.SearchedCase(mainCases.toList, elseCase)

inline def lit(x: () => java.io.InputStream): Expr[() => java.io.InputStream, NonScalarExpr] = Expr.ByteStreamLit(x)
inline def lit(x: java.io.InputStream): Expr[() => java.io.InputStream, NonScalarExpr] = Expr.ByteStreamLit(() => x)
inline def lit(x: Array[Byte]): Expr[Array[Byte], NonScalarExpr] = Expr.BytesLit(x)
inline def lit(x: Int): Expr[Int, NonScalarExpr] & LiteralExpression = Expr.IntLit(x)
inline def lit(x: Long): Expr[Long, NonScalarExpr] & LiteralExpression = Expr.LongLit(x)
inline def lit(x: Double): Expr[Double, NonScalarExpr] & LiteralExpression = Expr.DoubleLit(x)
inline def lit(x: Float): Expr[Float, NonScalarExpr] & LiteralExpression = Expr.FloatLit(x)
inline def lit(x: String): Expr[String, NonScalarExpr] & LiteralExpression = Expr.StringLit(x)
inline def lit(x: Boolean): Expr[Boolean, NonScalarExpr] & LiteralExpression = Expr.BooleanLit(x)
inline def lit(x: java.time.LocalDate): Expr[java.time.LocalDate, NonScalarExpr] & LiteralExpression =
  Expr.LocalDateLit(x)
inline def lit(x: java.time.LocalDateTime): Expr[java.time.LocalDateTime, NonScalarExpr] & LiteralExpression =
  Expr.LocalDateTimeLit(x)
inline def True = Expr.BooleanLit(true)
inline def False = Expr.BooleanLit(false)
inline def Null = Expr.NullLit[scala.Null]()
inline def lit[A]
  (x: List[A])
  (using ResultTag[A], ResultTag[List[A]], SimpleTypeResultTag[A])
  : Expr[List[A], NonScalarExpr] =
  Expr.ListExpr(x.map(dynamicLit(_)))

private def dynamicLit[A](x: A)(using SimpleTypeResultTag[A], ResultTag[A]): Expr[A, NonScalarExpr] =
  (x match
    case x if x == null                               => Null
    case x if x.isInstanceOf[Int]                     => lit(x.asInstanceOf[Int])
    case x if x.isInstanceOf[Long]                    => lit(x.asInstanceOf[Long])
    case x if x.isInstanceOf[Double]                  => lit(x.asInstanceOf[Double])
    case x if x.isInstanceOf[Float]                   => lit(x.asInstanceOf[Float])
    case x if x.isInstanceOf[String]                  => lit(x.asInstanceOf[String])
    case x if x.isInstanceOf[Boolean]                 => lit(x.asInstanceOf[Boolean])
    case x if x.isInstanceOf[java.time.LocalDate]     => lit(x.asInstanceOf[java.time.LocalDate])
    case x if x.isInstanceOf[java.time.LocalDateTime] => lit(x.asInstanceOf[java.time.LocalDateTime])
    case _ => assert(false, "This was supposed to be handled for every SimpleTypeResultTag!")
  ).asInstanceOf[Expr[A, NonScalarExpr]]

def Null[T](using ResultTag[T]) = Expr.NullLit[T]()
private case class ElseToken()
val Else = new ElseToken()
val CurrentTime = Expr.FunctionCall0NoParentheses[java.time.LocalDateTime]("CURRENT_TIMESTAMP")
val CurrentDate = Expr.FunctionCall0NoParentheses[java.time.LocalDate]("CURRENT_DATE")

def Var[T](thunk: => T)(using ResultTag[T], SimpleTypeResultTag[T]): Expr[T, NonScalarExpr] = Expr.VariableInput(() => {
  thunk
})
