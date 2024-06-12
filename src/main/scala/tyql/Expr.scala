package tyql

import scala.annotation.targetName
import language.experimental.namedTuples
import NamedTuple.{NamedTuple, AnyNamedTuple}
import scala.compiletime.error


type And[X, Y] <: Boolean = (X, Y) match // is this needed? Want an intersection, but true & false should reduce to false, not the type true & false
  case (true, true) => true
  case _ => false
type AllTrue[T <: Tuple] <: Boolean = T match
  case EmptyTuple => true
  case true *: t => AllTrue[t]
  case false *: _ => false
//type AndLambda = [X <: Boolean, Y <: Boolean] =>> And[X, Y]

//type And = [X <: Boolean, Y <: Boolean] =>> (X, Y) match // is this needed? Want an intersection, but true & false should reduce to false, not the type true & false
//  case (true, true) => true
//  case _ => false

/** The type of expressions in the query language */
trait Expr[Result, Scalar <: Boolean](using val tag: ResultTag[Result]) extends Selectable:

  type SubExpr[R] <: Expr[R, ?] = Scalar match
    case true => Expr[R, true]
    case _ => Expr[R, false]
  /** This type is used to support selection with any of the field names
   *  defined by Fields. Keep the same Scalar property as parent tuple.
   */
  type Fields = NamedTuple.Map[NamedTuple.From[Result], [T] =>> Expr[T, true]] // TODO: for now, accessing an attribute is considered always scalar.

 /** A selection of a field name defined by Fields is implemented by `selectDynamic`.
   *  The implementation will add a cast to the right Expr type corresponding
   *  to the field type.
   */
  def selectDynamic(fieldName: String) = Expr.Select(this, fieldName)

  /** Member methods to implement universal equality on Expr level. */
  def ==[OtherScalar <: Boolean] (other: Expr[?, OtherScalar]): Expr[Boolean, And[Scalar, OtherScalar]] = Expr.Eq(this, other)
//  def != (other: Expr[?]): Expr[Boolean] = Expr.Ne(this, other)

//  def == (other: String): Expr[Boolean] = Expr.Eq(this, Expr.StringLit(other))
//  def == (other: Int): Expr[Boolean] = Expr.Eq(this, Expr.IntLit(other))

object Expr:
//  def sum(x: Expr[Int]): Aggregation[Int] = Aggregation.Sum(x) // TODO: require summable type?
//  @targetName("doubleSum")
//  def sum(x: Expr[Double]): Aggregation[Double] = Aggregation.Sum(x) // TODO: require summable type?
  def sum[T: ResultTag, S <: true](x: Expr[T, S]): Aggregation[T] = Aggregation.Sum(x)
//  def avg[T: ResultTag](x: Expr[T]): Aggregation[T] = Aggregation.Avg(x)
//  def max[T: ResultTag](x: Expr[T]): Aggregation[T] = Aggregation.Max(x)
//  def min[T: ResultTag](x: Expr[T]): Aggregation[T] = Aggregation.Min(x)

  /** Sample extension methods for individual types */
  extension[Scalar <: Boolean] (x: Expr[Int, Scalar])
    def >[OtherScalar <: Boolean] (y: Expr[Int, OtherScalar]): Expr[Boolean, And[Scalar, OtherScalar]] = Gt(x, y)
//    def > (y: Int): Expr[Boolean] = Gt(x, IntLit(y))

//  // TODO: write for numerical
//  extension (x: Expr[Double])
//    @targetName("gtDoubleExpr")
//    def > (y: Expr[Double]): Expr[Boolean] = GtDouble(x, y)
//    @targetName("gtDoubleLit")
//    def > (y: Double): Expr[Boolean] = GtDouble(x, DoubleLit(y))
//
//  extension (x: Expr[Boolean])
//    def && (y: Expr[Boolean]): Expr[Boolean] = And(x, y)
//    def || (y: Expr[Boolean]): Expr[Boolean] = Or(x, y)
//
//  extension (x: Expr[String])
//    def toLowerCase: Expr[String] = Expr.Lower(x)
//    def toUpperCase: Expr[String] = Expr.Upper(x)
//
  // Note: All field names of constructors in the query language are prefixed with `$`
  // so that we don't accidentally pick a field name of a constructor class where we want
  // a name in the domain model instead.

  // Some sample constructors for Exprs
  case class Gt[S1 <: Boolean, S2 <: Boolean]($x: Expr[Int, S1], $y: Expr[Int, S2]) extends Expr[Boolean, And[S1, S2]]
//  case class GtDouble($x: Expr[Double], $y: Expr[Double]) extends Expr[Boolean]
//
//  case class Plus($x: Expr[Int], $y: Expr[Int]) extends Expr[Int]
//  case class And($x: Expr[Boolean], $y: Expr[Boolean]) extends Expr[Boolean]
//  case class Or($x: Expr[Boolean], $y: Expr[Boolean]) extends Expr[Boolean]
//
//  case class Upper($x: Expr[String]) extends Expr[String]
//  case class Lower($x: Expr[String]) extends Expr[String]

  // So far Select is weakly typed, so `selectDynamic` is easy to implement.
  // Todo: Make it strongly typed like the other cases
  case class Select[A: ResultTag, S <: Boolean]($x: Expr[A, S], $name: String) extends Expr

//  case class Single[S <: String, A]($x: Expr[A])(using ResultTag[NamedTuple[S *: EmptyTuple, A *: EmptyTuple]]) extends Expr[NamedTuple[S *: EmptyTuple, A *: EmptyTuple]]

//  case class Concat[A <: AnyNamedTuple, B <: AnyNamedTuple]($x: Expr[A], $y: Expr[B])(using ResultTag[NamedTuple.Concat[A, B]]) extends Expr[NamedTuple.Concat[A, B]]

  type StripExpr[E] = E match
    case Expr[b, _] => b

  type StripScalar[E] = E match
    case Expr[_, s] => s

//  type AllScalar[A <: AnyNamedTuple] = Tuple.Fold[Tuple.Map[NamedTuple.DropNames[A], StripScalar], true, AllTrue]

  case class Project[A <: AnyNamedTuple]($a: A)(using ResultTag[NamedTuple.Map[A, StripExpr]]) extends Expr[NamedTuple.Map[A, StripExpr], AllTrue[NamedTuple.DropNames[A]]]


// Also weakly typed in the arguments since these two classes model universal equality */
  case class Eq[S1 <: Boolean, S2 <: Boolean]($x: Expr[?, S1], $y: Expr[?, S2]) extends Expr[Boolean, And[S1, S2]]
//  case class Ne($x: Expr[?], $y: Expr[?]) extends Expr[Boolean]

  /** References are placeholders for parameters */
  private var refCount = 0 // TODO: do we want to recount from 0 for each query?

  case class Ref[A: ResultTag, S <: Boolean]($name: String = "") extends Expr[A, S]:
    private val id = refCount
    refCount += 1
    override def toString = s"ref$id(${$name})"

  /** Literals are type-specific, tailored to the types that the DB supports */
  case class IntLit($value: Int) extends Expr[Int, true]
  /** Scala values can be lifted into literals by conversions */
  given Conversion[Int, IntLit] = IntLit(_)

  case class StringLit($value: String) extends Expr[String, true]
  given Conversion[String, StringLit] = StringLit(_)

  case class DoubleLit($value: Double) extends Expr[Double, true]
  given Conversion[Double, DoubleLit] = DoubleLit(_)

  /** The internal representation of a function `A => B`
   *  Query languages are ususally first-order, so Fun is not an Expr
   */
  case class Fun[A, B]($param: Ref[A, ?], $f: B)
//  case class AggFun[A, B]($param: Ref[A], $f: B)

  type Pred[A, S <: Boolean] = Fun[A, Expr[Boolean, S]]

  type IsTupleOfExpr[A <: AnyNamedTuple] = Tuple.Union[NamedTuple.DropNames[A]] <:< Expr[?, ?]

  /** Explicit conversion from
   *      (name_1: Expr[T_1], ..., name_n: Expr[T_n])
   *  to
   *      Expr[(name_1: T_1, ..., name_n: T_n)]
   */
  extension [A <: AnyNamedTuple : IsTupleOfExpr](x: A)
    def toRow(using ResultTag[NamedTuple.Map[A, StripExpr]]) = ???//: Project[A] = Project(x)

// TODO: use NamedTuple.from to convert case classes to named tuples before using concat
//  extension [A <: AnyNamedTuple](x: Expr[A])
//    def concat[B <: AnyNamedTuple](other: Expr[B])(using ResultTag[NamedTuple.Concat[A, B]]) = Concat(x, other)

  /** Same as _.toRow, as an implicit conversion */
//  given [A <: AnyNamedTuple : IsTupleOfExpr](using ResultTag[NamedTuple.Map[A, StripExpr]]): Conversion[A, Expr.Project[A]] = Expr.Project(_)

end Expr
