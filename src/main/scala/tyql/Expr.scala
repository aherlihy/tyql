package tyql

import scala.annotation.targetName
import language.experimental.namedTuples
import NamedTuple.{NamedTuple, AnyNamedTuple}


/** The type of expressions in the query language */
trait Expr[Result](using val tag: ResultTag[Result]) extends Selectable:
  /** This type is used to support selection with any of the field names
   *  defined by Fields.
   */
  type Fields = NamedTuple.Map[NamedTuple.From[Result], Expr]

 /** A selection of a field name defined by Fields is implemented by `selectDynamic`.
   *  The implementation will add a cast to the right Expr type corresponding
   *  to the field type.
   */
  def selectDynamic(fieldName: String) = Expr.Select(this, fieldName)

  /** Member methods to implement universal equality on Expr level. */
  def == (other: Expr[?]): Expr[Boolean] = Expr.Eq(this, other)
  def != (other: Expr[?]): Expr[Boolean] = Expr.Ne(this, other)

  def == (other: String): Expr[Boolean] = Expr.Eq(this, Expr.StringLit(other))
  def == (other: Int): Expr[Boolean] = Expr.Eq(this, Expr.IntLit(other))

/**
 * Necessary to distinguish expressions that are NOT aggregations. Scalar expressions
 * produce one element per input row, as opposed to aggregate expressions produce one
 * element for the entire table.
 * @tparam Result
 */
//trait ScalarExpr[Result](using override val tag: ResultTag[Result]) extends Expr[Result]

object Expr:
  def sum(x: Expr[Int]): Aggregation[Int] = Aggregation.Sum(x) // TODO: require summable type?
  @targetName("doubleSum")
  def sum(x: Expr[Double]): Aggregation[Double] = Aggregation.Sum(x) // TODO: require summable type?
  def avg[T: ResultTag](x: Expr[T]): Aggregation[T] = Aggregation.Avg(x)
  def max[T: ResultTag](x: Expr[T]): Aggregation[T] = Aggregation.Max(x)
  def min[T: ResultTag](x: Expr[T]): Aggregation[T] = Aggregation.Min(x)

  /** Sample extension methods for individual types */
  extension (x: Expr[Int])
    def > (y: Expr[Int]): Expr[Boolean] = Gt(x, y)
    def > (y: Int): Expr[Boolean] = Gt(x, IntLit(y))

  // TODO: write for numerical
  extension (x: Expr[Double])
    @targetName("gtDoubleExpr")
    def > (y: Expr[Double]): Expr[Boolean] = GtDouble(x, y)
    @targetName("gtDoubleLit")
    def > (y: Double): Expr[Boolean] = GtDouble(x, DoubleLit(y))

  extension (x: Expr[Boolean])
    def && (y: Expr[Boolean]): Expr[Boolean] = And(x, y)
    def || (y: Expr[Boolean]): Expr[Boolean] = Or(x, y)

  extension (x: Expr[String])
    def toLowerCase: Expr[String] = Expr.Lower(x)
    def toUpperCase: Expr[String] = Expr.Upper(x)

  // Note: All field names of constructors in the query language are prefixed with `$`
  // so that we don't accidentally pick a field name of a constructor class where we want
  // a name in the domain model instead.

  // Some sample constructors for Exprs
  case class Gt($x: Expr[Int], $y: Expr[Int]) extends Expr[Boolean]
  case class GtDouble($x: Expr[Double], $y: Expr[Double]) extends Expr[Boolean]

  case class Plus($x: Expr[Int], $y: Expr[Int]) extends Expr[Int]
  case class And($x: Expr[Boolean], $y: Expr[Boolean]) extends Expr[Boolean]
  case class Or($x: Expr[Boolean], $y: Expr[Boolean]) extends Expr[Boolean]

  case class Upper($x: Expr[String]) extends Expr[String]
  case class Lower($x: Expr[String]) extends Expr[String]

  // So far Select is weakly typed, so `selectDynamic` is easy to implement.
  // Todo: Make it strongly typed like the other cases
  case class Select[A: ResultTag]($x: Expr[A], $name: String) extends Expr[A]

  case class Single[S <: String, A]($x: Expr[A])(using ResultTag[NamedTuple[S *: EmptyTuple, A *: EmptyTuple]]) extends Expr[NamedTuple[S *: EmptyTuple, A *: EmptyTuple]]

  case class Concat[A <: AnyNamedTuple, B <: AnyNamedTuple]($x: Expr[A], $y: Expr[B])(using ResultTag[NamedTuple.Concat[A, B]]) extends Expr[NamedTuple.Concat[A, B]]

  case class Project[A <: AnyNamedTuple]($a: A)(using ResultTag[NamedTuple.Map[A, StripExpr]]) extends Expr[NamedTuple.Map[A, StripExpr]]

  type StripExpr[E] = E match
    case Expr[b] => b

  // Also weakly typed in the arguments since these two classes model universal equality */
  case class Eq($x: Expr[?], $y: Expr[?]) extends Expr[Boolean]
  case class Ne($x: Expr[?], $y: Expr[?]) extends Expr[Boolean]

  case class Contains[A]($this: Query[A], $other: Expr[A]) extends Expr[Boolean]

  case class IsEmpty[A]($this: Query[A]) extends Expr[Boolean]

  case class NonEmpty[A]($this: Query[A]) extends Expr[Boolean]

  /** References are placeholders for parameters */
  private var refCount = 0 // TODO: do we want to recount from 0 for each query?

  case class Ref[A: ResultTag]() extends Expr[A]:
    private val id = refCount
    refCount += 1
    def stringRef() = s"ref$id"
    override def toString: String = s"Ref[${stringRef()}]"

  /** Literals are type-specific, tailored to the types that the DB supports */
  case class IntLit($value: Int) extends Expr[Int]
  /** Scala values can be lifted into literals by conversions */
  given Conversion[Int, IntLit] = IntLit(_)

  case class StringLit($value: String) extends Expr[String]
  given Conversion[String, StringLit] = StringLit(_)

  case class DoubleLit($value: Double) extends Expr[Double]
  given Conversion[Double, DoubleLit] = DoubleLit(_)

  /** The internal representation of a function `A => B`
   *  Query languages are ususally first-order, so Fun is not an Expr
   */
  case class Fun[A, B]($param: Ref[A], $body: B)
//  case class AggFun[A, B]($param: Ref[A], $f: B)

  type Pred[A] = Fun[A, Expr[Boolean]]

  type IsTupleOfExpr[A <: AnyNamedTuple] = Tuple.Union[NamedTuple.DropNames[A]] <:< Expr[?]

  /** Explicit conversion from
   *      (name_1: Expr[T_1], ..., name_n: Expr[T_n])
   *  to
   *      Expr[(name_1: T_1, ..., name_n: T_n)]
   */
  extension [A <: AnyNamedTuple : IsTupleOfExpr](x: A)
    def toRow(using ResultTag[NamedTuple.Map[A, StripExpr]]): Project[A] = Project(x)

// TODO: use NamedTuple.from to convert case classes to named tuples before using concat
  extension [A <: AnyNamedTuple](x: Expr[A])
    def concat[B <: AnyNamedTuple](other: Expr[B])(using ResultTag[NamedTuple.Concat[A, B]]) = Concat(x, other)

  /** Same as _.toRow, as an implicit conversion */
  given [A <: AnyNamedTuple : IsTupleOfExpr](using ResultTag[NamedTuple.Map[A, StripExpr]]): Conversion[A, Expr.Project[A]] = Expr.Project(_)

end Expr
