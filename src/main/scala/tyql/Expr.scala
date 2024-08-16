package tyql

import scala.annotation.targetName
import language.experimental.namedTuples
import NamedTuple.{NamedTuple, AnyNamedTuple}

trait ExprShape
class ScalarExpr extends ExprShape
class NExpr extends ExprShape

type CalculatedShape[S1, S2] <: ExprShape = S1 match
  case ScalarExpr => ScalarExpr
  case NExpr => S2 match
    case ScalarExpr => ScalarExpr
    case NExpr => NExpr


/** The type of expressions in the query language */
trait Expr[Result, Shape <: ExprShape](using val tag: ResultTag[Result]) extends Selectable:
  /** This type is used to support selection with any of the field names
   *  defined by Fields.
   */
  type Fields = //NamedTuple.Map[NamedTuple.From[Result], Expr]
                NamedTuple.Map[NamedTuple.From[Result], [T] =>> Expr[T, Shape]]

 /** A selection of a field name defined by Fields is implemented by `selectDynamic`.
   *  The implementation will add a cast to the right Expr type corresponding
   *  to the field type.
   */
  def selectDynamic(fieldName: String) = Expr.Select(this, fieldName)

  /** Member methods to implement universal equality on Expr level. */
  @targetName("eqNonScalar")
  def ==(other: Expr[?, NExpr]): Expr[Boolean, CalculatedShape[Shape, NExpr]] = Expr.Eq[Shape, NExpr](this, other)
  @targetName("eqScalar")
  def ==(other: Expr[?, ScalarExpr]): Expr[Boolean, CalculatedShape[Shape, ScalarExpr]] = Expr.Eq[Shape, ScalarExpr](this, other)

  @targetName("neqNonScalar")
  def != (other: Expr[?, NExpr]): Expr[Boolean, CalculatedShape[Shape, NExpr]] = Expr.Ne[Shape, NExpr](this, other)
  @targetName("neqScalar")
  def != (other: Expr[?, ScalarExpr]): Expr[Boolean, CalculatedShape[Shape, ScalarExpr]] = Expr.Ne[Shape, ScalarExpr](this, other)

  def == (other: String): Expr[Boolean, CalculatedShape[Shape, NExpr]] = Expr.Eq(this, Expr.StringLit(other))
  def == (other: Int): Expr[Boolean, CalculatedShape[Shape, NExpr]] = Expr.Eq(this, Expr.IntLit(other))

/**
 * Necessary to distinguish expressions that are NOT aggregations. Scalar expressions
 * produce one element per input row, as opposed to aggregate expressions produce one
 * element for the entire table.
 * @tparam Result
 */
//trait ScalarExpr[Result](using override val tag: ResultTag[Result]) extends Expr[Result]

object Expr:
  def sum(x: Expr[Int, ?]): AggregationExpr[Int] = AggregationExpr.Sum(x) // TODO: require summable type?
  @targetName("doubleSum")
  def sum(x: Expr[Double, ?]): AggregationExpr[Double] = AggregationExpr.Sum(x) // TODO: require summable type?
  def avg[T: ResultTag](x: Expr[T, ?]): AggregationExpr[T] = AggregationExpr.Avg(x)
  def max[T: ResultTag](x: Expr[T, ?]): AggregationExpr[T] = AggregationExpr.Max(x)
  def min[T: ResultTag](x: Expr[T,  ?]): AggregationExpr[T] = AggregationExpr.Min(x)

  /** Sample extension methods for individual types */
  extension [S1 <: ExprShape](x: Expr[Int, S1])
    def >[S2 <: ExprShape] (y: Expr[Int, S2]): Expr[Boolean, CalculatedShape[S1, S2]] = Gt(x, y)
    def >(y: Int): Expr[Boolean, CalculatedShape[S1, NExpr]] = Gt(x, IntLit(y))

  // TODO: write for numerical
  extension [S1 <: ExprShape](x: Expr[Double, S1])
    @targetName("gtDoubleExpr")
    def >[S2 <: ExprShape](y: Expr[Double, S2]): Expr[Boolean, CalculatedShape[S1, S2]] = GtDouble(x, y)
    @targetName("gtDoubleLit")
    def >(y: Double): Expr[Boolean, CalculatedShape[S1, NExpr]] = GtDouble(x, DoubleLit(y))

  extension [S1 <: ExprShape](x: Expr[Boolean, S1])
    def &&[S2 <: ExprShape] (y: Expr[Boolean, S2]): Expr[Boolean, CalculatedShape[S1, S2]] = And(x, y)
    def ||[S2 <: ExprShape] (y: Expr[Boolean, S2]): Expr[Boolean, CalculatedShape[S1, S2]] = Or(x, y)

  extension [S1 <: ExprShape](x: Expr[String, S1])
    def toLowerCase: Expr[String, CalculatedShape[S1, NExpr]] = Expr.Lower(x)
    def toUpperCase: Expr[String, CalculatedShape[S1, NExpr]] = Expr.Upper(x)

  // Note: All field names of constructors in the query language are prefixed with `$`
  // so that we don't accidentally pick a field name of a constructor class where we want
  // a name in the domain model instead.

  // Some sample constructors for Exprs
  case class Gt[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[Int, S1], $y: Expr[Int, S2]) extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class GtDouble[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[Double, S1], $y: Expr[Double, S2]) extends Expr[Boolean, CalculatedShape[S1, S2]]

  case class Plus[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[Int, S1], $y: Expr[Int, S2]) extends Expr[Int, CalculatedShape[S1, S2]]
  case class And[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[Boolean, S1], $y: Expr[Boolean, S2]) extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class Or[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[Boolean, S1], $y: Expr[Boolean, S2]) extends Expr[Boolean, CalculatedShape[S1, S2]]

  case class Upper[S <: ExprShape]($x: Expr[String, S]) extends Expr[String, CalculatedShape[S, NExpr]]
  case class Lower[S <: ExprShape]($x: Expr[String, S]) extends Expr[String, CalculatedShape[S, NExpr]]

  // So far Select is weakly typed, so `selectDynamic` is easy to implement.
  // Todo: Make it strongly typed like the other cases
  case class Select[A: ResultTag]($x: Expr[A, ?], $name: String) extends Expr[A, NExpr]

//  case class Single[S <: String, A]($x: Expr[A])(using ResultTag[NamedTuple[S *: EmptyTuple, A *: EmptyTuple]]) extends Expr[NamedTuple[S *: EmptyTuple, A *: EmptyTuple]]

  case class Concat[A <: AnyNamedTuple, B <: AnyNamedTuple, S1 <: ExprShape, S2 <: ExprShape]($x: Expr[A, S1], $y: Expr[B, S2])(using ResultTag[NamedTuple.Concat[A, B]]) extends Expr[NamedTuple.Concat[A, B], CalculatedShape[S1, S2]]

  case class Project[A <: AnyNamedTuple]($a: A)(using ResultTag[NamedTuple.Map[A, StripExpr]]) extends Expr[NamedTuple.Map[A, StripExpr], NExpr]

  type StripExpr[E] = E match
    case Expr[b, s] => b

  // Also weakly typed in the arguments since these two classes model universal equality */
  case class Eq[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[?, S1], $y: Expr[?, S2]) extends Expr[Boolean, CalculatedShape[S1, S2]]
  case class Ne[S1 <: ExprShape, S2 <: ExprShape]($x: Expr[?, S1], $y: Expr[?, S2]) extends Expr[Boolean, CalculatedShape[S1, S2]]

  case class Contains[A]($this: Query[A], $other: Expr[A, ?]) extends Expr[Boolean, ScalarExpr]

  case class IsEmpty[A]($this: Query[A]) extends Expr[Boolean, ScalarExpr]

  case class NonEmpty[A]($this: Query[A]) extends Expr[Boolean, ScalarExpr]

  /** References are placeholders for parameters */
  private var refCount = 0 // TODO: do we want to recount from 0 for each query?

  case class Ref[A: ResultTag, S<: ExprShape]() extends Expr[A, S]:
    private val id = refCount
    refCount += 1
    def stringRef() = s"ref$id"
    override def toString: String = s"Ref[${stringRef()}]"

  /** Literals are type-specific, tailored to the types that the DB supports */
  case class IntLit($value: Int) extends Expr[Int, NExpr]
  /** Scala values can be lifted into literals by conversions */
  given Conversion[Int, IntLit] = IntLit(_)

  case class StringLit($value: String) extends Expr[String, NExpr]
  given Conversion[String, StringLit] = StringLit(_)

  case class DoubleLit($value: Double) extends Expr[Double, NExpr]
  given Conversion[Double, DoubleLit] = DoubleLit(_)

  /** The internal representation of a function `A => B`
   *  Query languages are ususally first-order, so Fun is not an Expr
   */
  case class Fun[A, B, S <: ExprShape]($param: Ref[A, S], $body: B)
//  case class AggFun[A, B]($param: Ref[A], $f: B)

  type Pred[A, S <: ExprShape] = Fun[A, Expr[Boolean, S], S]

  type IsTupleOfExpr[A <: AnyNamedTuple] = Tuple.Union[NamedTuple.DropNames[A]] <:< Expr[?, NExpr]

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
