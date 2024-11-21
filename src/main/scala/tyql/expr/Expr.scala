package tyql

import scala.annotation.targetName
import language.experimental.namedTuples
import NamedTuple.{AnyNamedTuple, NamedTuple}
import scala.deriving.*
import scala.compiletime.{erasedValue, summonInline}
import tyql.DialectFeature

// TODO: probably seal
trait ExprShape
class ScalarExpr extends ExprShape
class NonScalarExpr extends ExprShape

type CalculatedShape[S1 <: ExprShape, S2 <: ExprShape] <: ExprShape = S2 match
  case ScalarExpr => S2
  case NonScalarExpr => S1

/** The type of expressions in the query language */
trait Expr[Result, Shape <: ExprShape](using val tag: ResultTag[Result]) extends Selectable:
  /** This type is used to support selection with any of the field names
   *  defined by Fields.
   */
  type Fields = NamedTuple.Map[NamedTuple.From[Result], [T] =>> Expr[T, Shape]]

 /** A selection of a field name defined by Fields is implemented by `selectDynamic`.
   *  The implementation will add a cast to the right Expr type corresponding
   *  to the field type.
   */
  def selectDynamic(fieldName: String) = Expr.Select(this, fieldName)

  /** Member methods to implement universal equality on Expr level. */
  @targetName("eqNonScalar")
  def ==(other: Expr[?, NonScalarExpr]): Expr[Boolean, Shape] = Expr.Eq[Shape, NonScalarExpr](this, other)
  @targetName("eqScalar")
  def ==(other: Expr[?, ScalarExpr]): Expr[Boolean, ScalarExpr] = Expr.Eq[Shape, ScalarExpr](this, other)
//  def == [S <: ScalarExpr](other: Expr[?, S]): Expr[Boolean, CalculatedShape[Shape, S]] = Expr.Eq(this, other)
  def ==(other: String): Expr[Boolean, Shape] = Expr.Eq(this, Expr.StringLit(other))
  def ==(other: Int): Expr[Boolean, Shape] = Expr.Eq(this, Expr.IntLit(other))
  def ==(other: Boolean): Expr[Boolean, Shape] = Expr.Eq(this, Expr.BooleanLit(other))

  @targetName("neqNonScalar")
  def != (other: Expr[?, NonScalarExpr]): Expr[Boolean, Shape] = Expr.Ne[Shape, NonScalarExpr](this, other)
  @targetName("neqScalar")
  def != (other: Expr[?, ScalarExpr]): Expr[Boolean, ScalarExpr] = Expr.Ne[Shape, ScalarExpr](this, other)

  def cases[DestinationT: ResultTag, SV <: ExprShape](firstCase: (Expr[Result, Shape] | ElseToken, Expr[DestinationT, SV]), restOfCases: (Expr[Result, Shape] | ElseToken, Expr[DestinationT, SV])*): Expr[DestinationT, SV] =
    type FromT = Result
    var mainCases: collection.mutable.ArrayBuffer[(Expr[FromT, Shape], Expr[DestinationT, SV])] = collection.mutable.ArrayBuffer.empty
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
    // def >[S2 <: ExprShape](y: Expr[Int, S2]): Expr[Boolean, CalculatedShape[S1, S2]] = Gt(x, y)
    def >(y: Expr[Int, ScalarExpr]): Expr[Boolean, CalculatedShape[S1, ScalarExpr]] = Gt(x, y)
    @targetName("gtIntNonscalar")
    def >(y: Expr[Int, NonScalarExpr]): Expr[Boolean, CalculatedShape[S1, NonScalarExpr]] = Gt(x, y)
    def >(y: Int): Expr[Boolean, S1] = Gt[S1, NonScalarExpr](x, IntLit(y))
    // def <[S2 <: ExprShape] (y: Expr[Int, S2]): Expr[Boolean, CalculatedShape[S1, S2]] = Lt(x, y)
    def <(y: Expr[Int, ScalarExpr]): Expr[Boolean, CalculatedShape[S1, ScalarExpr]] = Lt(x, y)
    @targetName("ltIntNonscalar")
    def <(y: Expr[Int, NonScalarExpr]): Expr[Boolean, CalculatedShape[S1, NonScalarExpr]] = Lt(x, y)
    def <(y: Int): Expr[Boolean, S1] = Lt[S1, NonScalarExpr](x, IntLit(y))
    def <=[S2 <: ExprShape] (y: Expr[Int, S2]): Expr[Boolean, CalculatedShape[S1, S2]] = Lte(x, y)
    def >=[S2 <: ExprShape] (y: Expr[Int, S2]): Expr[Boolean, CalculatedShape[S1, S2]] = Gte(x, y)
    @targetName("addIntScalar")
    def +(y: Expr[Int, ScalarExpr]): Expr[Int, CalculatedShape[S1, ScalarExpr]] = Plus(x, y)
    @targetName("addIntNonScalar")
    def +(y: Expr[Int, NonScalarExpr]): Expr[Int, CalculatedShape[S1, NonScalarExpr]] = Plus(x, y)
    def +(y: Int): Expr[Int, S1] = Plus[S1, NonScalarExpr, Int](x, IntLit(y))
    @targetName("subtractIntScalar")
    def -(y: Expr[Int, ScalarExpr]): Expr[Int, CalculatedShape[S1, ScalarExpr]] = Minus(x, y)
    @targetName("subtractIntNonScalar")
    def -(y: Expr[Int, NonScalarExpr]): Expr[Int, CalculatedShape[S1, NonScalarExpr]] = Minus(x, y)
    def -(y: Int): Expr[Int, S1] = Minus[S1, NonScalarExpr, Int](x, IntLit(y))
    @targetName("multiplyIntScalar")
    def *(y: Expr[Int, ScalarExpr]): Expr[Int, CalculatedShape[S1, ScalarExpr]] = Times(x, y)
    @targetName("multiplyIntNonScalar")
    def *(y: Expr[Int, NonScalarExpr]): Expr[Int, CalculatedShape[S1, NonScalarExpr]] = Times(x, y)
    def *(y: Int): Expr[Int, S1] = Times(x, IntLit(y))

  // TODO: write for numerical
  extension [S1 <: ExprShape](x: Expr[Double, S1])
    @targetName("gtDoubleScalar")
    def >(y: Expr[Double, ScalarExpr]): Expr[Boolean, CalculatedShape[S1, ScalarExpr]] = GtDouble(x, y)
    @targetName("gtDoubleNonScalar")
    def >(y: Expr[Double, NonScalarExpr]): Expr[Boolean, CalculatedShape[S1, NonScalarExpr]] = GtDouble(x, y)
    def >(y: Double): Expr[Boolean, S1] = GtDouble[S1, NonScalarExpr](x, DoubleLit(y))
    def <(y: Double): Expr[Boolean, S1] = LtDouble[S1, NonScalarExpr](x, DoubleLit(y))
    @targetName("addDouble")
    def +[S2 <: ExprShape](y: Expr[Double, S2]): Expr[Double, CalculatedShape[S1, S2]] = Plus(x, y)
    @targetName("multipleDouble")
    def *[S2 <: ExprShape](y: Expr[Double, S2]): Expr[Double, CalculatedShape[S1, S2]] = Times(x, y)
    def *(y: Double): Expr[Double, S1] = Times[S1, NonScalarExpr, Double](x, DoubleLit(y))

  extension [S1 <: ExprShape](x: Expr[Boolean, S1])
    def &&[S2 <: ExprShape] (y: Expr[Boolean, S2]): Expr[Boolean, CalculatedShape[S1, S2]] = And(x, y)
    def ||[S2 <: ExprShape] (y: Expr[Boolean, S2]): Expr[Boolean, CalculatedShape[S1, S2]] = Or(x, y)
    def unary_! = Not(x)
    def ^(y: Expr[Boolean, S1]): Expr[Boolean, S1] = Xor(x, y)

  extension [S1 <: ExprShape](x: Expr[String, S1])
    def toLowerCase: Expr[String, S1] = Expr.Lower(x)
    def toUpperCase: Expr[String, S1] = Expr.Upper(x)
    def charLength: Expr[Int, S1] = Expr.StringCharLength(x)
    def length: Expr[Int, S1] = charLength
    def byteLength: Expr[Int, S1] = Expr.StringByteLength(x)
    def stripLeading: Expr[String, S1] = Expr.LTrim(x)  // Java naming
    def stripTrailing: Expr[String, S1] = Expr.RTrim(x) // Java naming
    def strip: Expr[String, S1] = Expr.Trim(x)          // Java naming
    def ltrim: Expr[String, S1] = Expr.LTrim(x)         // SQL naming
    def rtrim: Expr[String, S1] = Expr.RTrim(x)         // SQL naming
    def trim: Expr[String, S1] = Expr.Trim(x)           // SQL naming
    def replace[S2 <: ExprShape](from: Expr[String, S2], to: Expr[String, S2]): Expr[String, CalculatedShape[S1, S2]] = Expr.StrReplace(x, from, to)
    // TODO maybe add assertions that len should be >= 0 and from >= 1 if we know them?
    // SQL semantics (1-based indexing, start+length)
    def substr[S2 <: ExprShape](from: Expr[Int, S2], len: Expr[Int, S2] = null): Expr[String, CalculatedShape[S1, S2]] = Expr.Substring(x, from, Option.fromNullable(len))
    // Java semantics (0-based indexing, start+afterLast)
    def substring[S2 <: ExprShape](start: Expr[Int, S2], afterLast: Expr[Int, S2] = null): Expr[String, CalculatedShape[S1, S2]] =
      if afterLast != null then
        substr(Expr.Plus(Expr.IntLit(1), start).asInstanceOf[Expr[Int, S2]], Expr.Minus(afterLast, start).asInstanceOf[Expr[Int, S2]]) // XXX how to avoid this cast
      else
        substr(Expr.Plus(Expr.IntLit(1), start).asInstanceOf[Expr[Int, S2]], null) // XXX how to avoid this cast
    def like[S2 <: ExprShape](pattern: Expr[String, S2]): Expr[Boolean, CalculatedShape[S1, S2]] = Expr.StrLike(x, pattern)
    def `+`[S2 <: ExprShape](y: Expr[String, S2]): Expr[String, CalculatedShape[S1, S2]] = Expr.StrConcat(x, Seq(y))
    def reverse(using DialectFeature.ReversibleStrings): Expr[String, S1] = Expr.StrReverse(x)

  def concat[S <: ExprShape](strs: Seq[Expr[String, S]]): Expr[String, S] =
    assert(strs.nonEmpty, "concat requires at least one argument")
    StrConcatUniform(strs.head, strs.tail)

  // TODO XXX this cannot be named concat since then Scala will never resolve it, it will always try for the first version without the sep parameter.
  def concatWith[S <: ExprShape, SS <: ExprShape](strs: Seq[Expr[String, S]], sep: Expr[String, SS]): Expr[String, CalculatedShape[S, SS]] =
    assert(strs.nonEmpty, "concatWith requires at least one argument")
    StrConcatSeparator(sep, strs.head, strs.tail)

  extension [A](x: Expr[List[A], NonScalarExpr])(using ResultTag[List[A]])
    def prepend(elem: Expr[A, NonScalarExpr]): Expr[List[A], NonScalarExpr] = ListPrepend(elem, x)
    def append(elem: Expr[A, NonScalarExpr]): Expr[List[A], NonScalarExpr] = ListAppend(x, elem)
    def contains(elem: Expr[A, NonScalarExpr]): Expr[Boolean, NonScalarExpr] = ListContains(x, elem)
    def length: Expr[Int, NonScalarExpr] = ListLength(x)

  // Aggregations
  def sum(x: Expr[Int, ?]): AggregationExpr[Int] = AggregationExpr.Sum(x) // TODO: require summable type?

  @targetName("doubleSum")
  def sum(x: Expr[Double, ?]): AggregationExpr[Double] = AggregationExpr.Sum(x) // TODO: require summable type?

  def avg[T: ResultTag](x: Expr[T, ?]): AggregationExpr[T] = AggregationExpr.Avg(x)

  @targetName("doubleAvg")
  def avg(x: Expr[Double, ?]): AggregationExpr[Double] = AggregationExpr.Avg(x)

  def max[T: ResultTag](x: Expr[T, ?]): AggregationExpr[T] = AggregationExpr.Max(x)

  def min[T: ResultTag](x: Expr[T, ?]): AggregationExpr[T] = AggregationExpr.Min(x)

  def count(x: Expr[Int, ?]): AggregationExpr[Int] = AggregationExpr.Count(x)
  @targetName("stringCnt")
  def count(x: Expr[String, ?]): AggregationExpr[Int] = AggregationExpr.Count(x)

  // TODO aren't these types too restrictive?
  def cases[T: ResultTag, SC <: ExprShape, SV <: ExprShape](firstCase: (Expr[Boolean, SC] | true | ElseToken, Expr[T, SV]), restOfCases: (Expr[Boolean, SC] | true | ElseToken, Expr[T, SV])*): Expr[T, SV] =
    var mainCases: collection.mutable.ArrayBuffer[(Expr[Boolean, SC], Expr[T, SV])] = collection.mutable.ArrayBuffer.empty
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
    SearchedCase(mainCases.toList, elseCase)

  // Note: All field names of constructors in the query language are prefixed with `$`
  // so that we don't accidentally pick a field name of a constructor class where we want
  // a name in the domain model instead.

  // Some sample constructors for Exprs
  case class Lt[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[Int, S1], $y: Expr[Int, S2]) extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class Lte[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[Int, S1], $y: Expr[Int, S2]) extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class Gte[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[Int, S1], $y: Expr[Int, S2]) extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class Gt[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[Int, S1], $y: Expr[Int, S2]) extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class GtDouble[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[Double, S1], $y: Expr[Double, S2]) extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class LtDouble[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[Double, S1], $y: Expr[Double, S2]) extends Expr[Boolean, CalculatedShape[S1, S2]]

  case class FunctionCall0[R](name: String)(using ResultTag[R]) extends Expr[R, NonScalarExpr] // XXX TODO NonScalarExpr?
  case class FunctionCall1[A1, R, S1 <: ExprShape](name: String, $a1: Expr[A1, S1])(using ResultTag[R]) extends Expr[R, S1]
  case class FunctionCall2[A1, A2, R, S1 <: ExprShape, S2 <: ExprShape](name: String, $a1: Expr[A1, S1], $a2: Expr[A2, S2])(using ResultTag[R]) extends Expr[R, CalculatedShape[S1, S2]]

  // TODO think about it again
  case class RawSQLInsert[R](sql: String, replacements: Map[String, Expr[?, ?]] = Map.empty)(using ResultTag[R]) extends Expr[R, NonScalarExpr] // XXX TODO NonScalarExpr?

  case class Plus[S1 <: ExprShape, S2 <: ExprShape, T: Numeric]($x: Expr[T, S1], $y: Expr[T, S2])(using ResultTag[T]) extends Expr[T, CalculatedShape[S1, S2]]
  case class Minus[S1 <: ExprShape, S2 <: ExprShape, T: Numeric]($x: Expr[T, S1], $y: Expr[T, S2])(using ResultTag[T]) extends Expr[T, CalculatedShape[S1, S2]]
  case class Times[S1 <: ExprShape, S2 <: ExprShape, T: Numeric]($x: Expr[T, S1], $y: Expr[T, S2])(using ResultTag[T]) extends Expr[T, CalculatedShape[S1, S2]]
  case class And[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[Boolean, S1], $y: Expr[Boolean, S2]) extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class Or[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[Boolean, S1], $y: Expr[Boolean, S2]) extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class Not[S1 <: ExprShape]($x: Expr[Boolean, S1]) extends Expr[Boolean, S1]
  case class Xor[S1 <: ExprShape]($x : Expr[Boolean, S1], $y : Expr[Boolean, S1]) extends Expr[Boolean, S1]

  case class Upper[S <: ExprShape]($x: Expr[String, S]) extends Expr[String, S]
  case class Lower[S <: ExprShape]($x: Expr[String, S]) extends Expr[String, S]
  case class StringCharLength[S <: ExprShape]($x: Expr[String, S]) extends Expr[Int, S]
  case class StringByteLength[S <: ExprShape]($x: Expr[String, S]) extends Expr[Int, S]
  case class Trim[S <: ExprShape]($x: Expr[String, S]) extends Expr[String, S]
  case class LTrim[S <: ExprShape]($x: Expr[String, S]) extends Expr[String, S]
  case class RTrim[S <: ExprShape]($x: Expr[String, S]) extends Expr[String, S]
  case class StrReplace[S <: ExprShape, S2 <: ExprShape]($s: Expr[String, S], $from: Expr[String, S2], $to: Expr[String, S2]) extends Expr[String, CalculatedShape[S, S2]]
  case class Substring[S <: ExprShape, S2 <: ExprShape]($s: Expr[String, S], $from: Expr[Int, S2], $len: Option[Expr[Int, S2]]) extends Expr[String, CalculatedShape[S, S2]]
  case class StrLike[S <: ExprShape, S2 <: ExprShape]($s: Expr[String, S], $pattern: Expr[String, S2]) extends Expr[Boolean, CalculatedShape[S, S2]] // NonScalar like StringLit
  case class StrConcat[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[String, S1], $xs: Seq[Expr[String, S2]]) extends Expr[String, CalculatedShape[S1, S2]] // First one has a different shape so you can use it as an opertor between two arguments that have different shapes
  case class StrConcatUniform[S1 <: ExprShape]($x: Expr[String, S1], $xs: Seq[Expr[String, S1]]) extends Expr[String, S1]
  case class StrConcatSeparator[S1 <: ExprShape, S3 <: ExprShape]($sep: Expr[String, S3], $x: Expr[String, S1], $xs: Seq[Expr[String, S1]]) extends Expr[String, CalculatedShape[S1, S3]]
  case class StrReverse[S <: ExprShape]($x: Expr[String, S]) extends Expr[String, S]

  case class RandomUUID() extends Expr[String, NonScalarExpr] // XXX NonScalarExpr?
  case class RandomFloat() extends Expr[Double, NonScalarExpr] // XXX NonScalarExpr?
  case class RandomInt[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[Int, S1], $y: Expr[Int, S2]) extends Expr[Int, CalculatedShape[S1, S2]]

  case class ListExpr[A]($elements: List[Expr[A, NonScalarExpr]])(using ResultTag[List[A]]) extends Expr[List[A], NonScalarExpr]
  extension [A, E <: Expr[A, NonScalarExpr]](x: List[E])
    def toExpr(using ResultTag[List[A]]): ListExpr[A] = ListExpr(x)
  //  given Conversion[List[A], ListExpr[A]] = ListExpr(_)

  case class ListPrepend[A]($x: Expr[A, NonScalarExpr], $list: Expr[List[A], NonScalarExpr])(using ResultTag[List[A]]) extends Expr[List[A], NonScalarExpr]
  case class ListAppend[A]($list: Expr[List[A], NonScalarExpr], $x: Expr[A, NonScalarExpr])(using ResultTag[List[A]]) extends Expr[List[A], NonScalarExpr]
  case class ListContains[A]($list: Expr[List[A], NonScalarExpr], $x: Expr[A, NonScalarExpr])(using ResultTag[Boolean]) extends Expr[Boolean, NonScalarExpr]
  case class ListLength[A]($list: Expr[List[A], NonScalarExpr])(using ResultTag[Int]) extends Expr[Int, NonScalarExpr]

  // So far Select is weakly typed, so `selectDynamic` is easy to implement.
  // TODO: Make it strongly typed like the other cases
  case class Select[A: ResultTag]($x: Expr[A, ?], $name: String) extends Expr[A, NonScalarExpr]

//  case class Single[S <: String, A]($x: Expr[A])(using ResultTag[NamedTuple[S *: EmptyTuple, A *: EmptyTuple]]) extends Expr[NamedTuple[S *: EmptyTuple, A *: EmptyTuple]]

  case class Concat[A <: AnyNamedTuple, B <: AnyNamedTuple, S1 <: ExprShape, S2 <: ExprShape]($x: Expr[A, S1], $y: Expr[B, S2])(using ResultTag[NamedTuple.Concat[A, B]]) extends Expr[NamedTuple.Concat[A, B], CalculatedShape[S1, S2]]

  case class Project[A <: AnyNamedTuple]($a: A)(using ResultTag[NamedTuple.Map[A, StripExpr]]) extends Expr[NamedTuple.Map[A, StripExpr], NonScalarExpr]

  type StripExpr[E] = E match
    case Expr[b, s] => b
    case AggregationExpr[b] => b

  // Also weakly typed in the arguments since these two classes model universal equality */
  case class Eq[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[?, S1], $y: Expr[?, S2]) extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class Ne[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[?, S1], $y: Expr[?, S2]) extends Expr[Boolean, CalculatedShape[S1, S2]]

  // Expressions resulting from queries
  // Cannot use Contains with an aggregation
  case class Contains[A]($this: Query[A, ?], $other: Expr[A, NonScalarExpr]) extends Expr[Boolean, NonScalarExpr]
  case class IsEmpty[A]($this: Query[A, ?]) extends Expr[Boolean, NonScalarExpr]
  case class NonEmpty[A]($this: Query[A, ?]) extends Expr[Boolean, NonScalarExpr]

  /** References are placeholders for parameters */
  private var refCount = 0 // TODO: do we want to recount from 0 for each query?
  private var exprRefCount = 0

  // References to relations
  case class Ref[A: ResultTag, S <: ExprShape](idx: Int = -1) extends Expr[A, S]:
    private val $id = refCount
    refCount += 1
    val idxStr = if idx == -1 then "" else s"_$idx"
    def stringRef() = s"ref${$id}$idxStr"
    override def toString: String = s"Ref[${stringRef()}]$idxStr"

  /** The internal representation of a function `A => B`
   * Query languages are usually first-order, so Fun is not an Expr
   */
  case class Fun[A, B, S <: ExprShape]($param: Ref[A, S], $body: B)

  // TODO aren't these types too restrictive?
  case class SearchedCase[T, SC <: ExprShape, SV <: ExprShape]($cases: List[(Expr[Boolean, SC], Expr[T, SV])], $else: Option[Expr[T, SV]])(using ResultTag[T]) extends Expr[T, SV]
  case class SimpleCase[TE, TR, SE <: ExprShape, SR <: ExprShape]($expr: Expr[TE, SE], $cases: List[(Expr[TE, SE], Expr[TR, SR])], $else: Option[Expr[TR, SR]])(using ResultTag[TE], ResultTag[TR]) extends Expr[TR, SR]

  /** Literals are type-specific, tailored to the types that the DB supports */
  case class IntLit($value: Int) extends Expr[Int, NonScalarExpr]
  /** Scala values can be lifted into literals by conversions */
  given Conversion[Int, IntLit] = IntLit(_)
  // XXX maybe only from literals with FromDigits?

  case class StringLit($value: String) extends Expr[String, NonScalarExpr] // TODO XXX why is this nonscalar?
  given Conversion[String, StringLit] = StringLit(_)

  case class DoubleLit($value: Double) extends Expr[Double, NonScalarExpr]
  given Conversion[Double, DoubleLit] = DoubleLit(_)

  case class BooleanLit($value: Boolean) extends Expr[Boolean, NonScalarExpr]
  //  given Conversion[Boolean, BooleanLit] = BooleanLit(_)
  // TODO why does this break things?

  def randomFloat(using r: DialectFeature.RandomFloat)(): Expr[Double, NonScalarExpr] =
    RandomFloat()

  def randomUUID(using r: DialectFeature.RandomUUID)(): Expr[String, NonScalarExpr] =
    RandomUUID()

  def randomInt[S1 <: ExprShape, S2 <: ExprShape](a: Expr[Int, S1], b: Expr[Int, S2])(using r: DialectFeature.RandomIntegerInInclusiveRange): Expr[Int, CalculatedShape[S1, S2]] =
    // TODO maybe add a check for (a <= b) if we know both components at generation time?
    // TODO what about parentheses? Do we really not need them?
    RandomInt(a, b)

  /** Should be able to rely on the implicit conversions, but not always.
   *  One approach is to overload, another is to provide a user-facing toExpr
   *  function.
   */
//  def toExpr[T](t: T): Expr[T, NonScalarExpr] = t match
//    case t:Int => IntLit(t)
//    case t:Double => DoubleLit(t)
//    case t:String => StringLit(t)
//    case t:Boolean => BooleanLit(t)

/* ABSTRACTION: if we want to abstract over expressions (not relations) in the DSL, to enable better composability,
  then the DSL needs some kind of abstraction/application operation.
  Option 1: (already supported) use host-level abstraction e.g. define a lambda.
  Option 2: (below) define a substitution method, WIP.
  Option 3: Use a macro to do substitution, but then lose the macro-free claim.
*/
  case class RefExpr[A: ResultTag, S <: ExprShape]() extends Expr[A, S]:
    private val id = exprRefCount
    exprRefCount += 1
    def stringRef() = s"exprRef$id"
    override def toString: String = s"ExprRef[${stringRef()}]"

  case class AbstractedExpr[A, B, S <: ExprShape]($param: RefExpr[A, S], $body: Expr[B, S]):
    def apply(exprArg: Expr[A, S]): Expr[B, S] =
      substitute($body, $param, exprArg)
    private def substitute[C](expr: Expr[B, S],
                              formalP: RefExpr[A, S],
                              actualP: Expr[A, S]): Expr[B, S] = ???
  type Pred[A, S <: ExprShape] = Fun[A, Expr[Boolean, S], S]

  type IsTupleOfExpr[A <: AnyNamedTuple] = Tuple.Union[NamedTuple.DropNames[A]] <:< Expr[?, NonScalarExpr]

  /** Explicit conversion from
   *      (name_1: Expr[T_1], ..., name_n: Expr[T_n])
   *  to
   *      Expr[(name_1: T_1, ..., name_n: T_n)]
   */
  extension [A <: AnyNamedTuple : IsTupleOfExpr](x: A)
    def toRow(using ResultTag[NamedTuple.Map[A, StripExpr]]): Project[A] = Project(x)

// TODO: use NamedTuple.from to convert case classes to named tuples before using concat
  extension [A <: AnyNamedTuple, S <: ExprShape](x: Expr[A, S])
    def concat[B <: AnyNamedTuple, S2 <: ExprShape](other: Expr[B,S2])(using ResultTag[NamedTuple.Concat[A, B]]): Expr[NamedTuple.Concat[A, B], CalculatedShape[S, S2]] = Concat(x, other)

  /** Same as _.toRow, as an implicit conversion */
//  given [A <: AnyNamedTuple : IsTupleOfExpr](using ResultTag[NamedTuple.Map[A, StripExpr]]): Conversion[A, Expr.Project[A]] = Expr.Project(_)

end Expr

// TODO where should this be?
def lit(x: Int): Expr[Int, NonScalarExpr] = Expr.IntLit(x)
def lit(x: Double): Expr[Double, NonScalarExpr] = Expr.DoubleLit(x)
def lit(x: String): Expr[String, NonScalarExpr] = Expr.StringLit(x)
def True = Expr.BooleanLit(true)
def False = Expr.BooleanLit(false)
private case class ElseToken()
val Else = new ElseToken()
