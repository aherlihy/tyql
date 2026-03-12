package tyql

import scala.annotation.targetName
import java.time.LocalDate
import language.experimental.namedTuples
import NamedTuple.{AnyNamedTuple, NamedTuple}
import scala.deriving.*
import scala.compiletime.{erasedValue, summonInline}

/** The type of expressions in the query language */
trait Expr[Result](using val tag: ResultTag[Result]) extends Selectable:
  /** This type is used to support selection with any of the field names
   *  defined by Fields.
   */
  type Fields = NamedTuple.Map[NamedTuple.From[Result], [T] =>> Expr[T]]

 /** A selection of a field name defined by Fields is implemented by `selectDynamic`.
   *  The implementation will add a cast to the right Expr type corresponding
   *  to the field type.
   */
  def selectDynamic(fieldName: String) = Expr.Select(this, fieldName)

  /** Member methods to implement universal equality on Expr level. */
  def ==(other: Expr[?]): Expr[Boolean] = Expr.Eq(this, other)
  def ==(other: String): Expr[Boolean] = Expr.Eq(this, Expr.StringLit(other))
  def ==(other: Int): Expr[Boolean] = Expr.Eq(this, Expr.IntLit(other))
  def ==(other: Boolean): Expr[Boolean] = Expr.Eq(this, Expr.BooleanLit(other))

  def != (other: Expr[?]): Expr[Boolean] = Expr.Ne(this, other)

object Expr:
  /** Sample extension methods for individual types */
  extension (x: Expr[Int])
    def >(y: Expr[Int]): Expr[Boolean] = Gt(x, y)
    def >(y: Int): Expr[Boolean] = Gt(x, IntLit(y))
    def <(y: Expr[Int]): Expr[Boolean] = Lt(x, y)
    def <(y: Int): Expr[Boolean] = Lt(x, IntLit(y))
    def <=(y: Expr[Int]): Expr[Boolean] = Lte(x, y)
    def <=(y: Int): Expr[Boolean] = Lte(x, IntLit(y))
    def >=(y: Expr[Int]): Expr[Boolean] = Gte(x, y)
    def >=(y: Int): Expr[Boolean] = Gte(x, IntLit(y))

  // TODO: write for numerical
  extension(x: Expr[Double])
    @targetName("gtDoubleExpr")
    def >(y: Expr[Double]): Expr[Boolean] = GtDouble(x, y)
    @targetName("gtDoubleLit")
    def >(y: Double): Expr[Boolean] = GtDouble(x, DoubleLit(y))
    def <(y: Double): Expr[Boolean] = LtDouble(x, DoubleLit(y))
    @targetName("lteDoubleLit")
    def <=(y: Double): Expr[Boolean] = LteDouble(x, DoubleLit(y))
    @targetName("lteDoubleExpr")
    def <=(y: Expr[Double]): Expr[Boolean] = LteDouble(x, y)
    @targetName("gteDoubleLit")
    def >=(y: Double): Expr[Boolean] = GteDouble(x, DoubleLit(y))
    @targetName("gteDoubleExpr")
    def >=(y: Expr[Double]): Expr[Boolean] = GteDouble(x, y)

    def +(y: Double): Expr[Double] = Plus[Double](x, DoubleLit(y))
    def -(y: Double): Expr[Double] = Minus[Double](x, DoubleLit(y))
    def *(y: Double): Expr[Double] = Times[Double](x, DoubleLit(y))


  extension(x: Expr[LocalDate])
    @targetName("gtDateExpr")
    def >(y: Expr[LocalDate]): Expr[Boolean] = GtDate(x, y)
    @targetName("gtDateLit")
    def >(y: LocalDate): Expr[Boolean] = GtDate(x, DateLit(y))
    @targetName("ltDateExpr")
    def <(y: Expr[LocalDate]): Expr[Boolean] = LtDate(x, y)
    @targetName("ltDateLit")
    def <(y: LocalDate): Expr[Boolean] = LtDate(x, DateLit(y))
    @targetName("lteDateExpr")
    def <=(y: Expr[LocalDate]): Expr[Boolean] = LteDate(x, y)
    @targetName("lteDateLit")
    def <=(y: LocalDate): Expr[Boolean] = LteDate(x, DateLit(y))
    @targetName("gteDateExpr")
    def >=(y: Expr[LocalDate]): Expr[Boolean] = GteDate(x, y)
    @targetName("gteDateLit")
    def >=(y: LocalDate): Expr[Boolean] = GteDate(x, DateLit(y))

  extension(x: Expr[Boolean])
    def &&(y: Expr[Boolean]): Expr[Boolean] = And(x, y)
    def ||(y: Expr[Boolean]): Expr[Boolean] = Or(x, y)
    def unary_! = Not(x)

  extension (x: Expr[String])
    def toLowerCase: Expr[String] = Expr.Lower(x)
    def toUpperCase: Expr[String] = Expr.Upper(x)

  extension(x: Expr[Double])
    @targetName("addDoubleToNonRestricted")
    def +(y: Expr[Double]): Expr[Double] = Plus(x, y)
    @targetName("subtractDoubleToNonRestricted")
    def -(y: Expr[Double]): Expr[Double] = Minus(x, y)
    @targetName("multipleDoubleToNonRestricted")
    def *(y: Expr[Double]): Expr[Double] = Times(x, y)
  extension(x: Expr[Int])
    @targetName("addDoubleToNonRestrictedInt")
    def +(y: Expr[Int]): Expr[Int] = Plus(x, y)
    @targetName("subtractDoubleToNonRestrictedInt")
    def -(y: Expr[Int]): Expr[Int] = Minus(x, y)
    @targetName("multipleDoubleToNonRestrictedInt")
    def *(y: Expr[Int]): Expr[Int] = Times(x, y)

  extension [A](x: Expr[List[A]])(using ResultTag[List[A]])
    def prepend(elem: Expr[A]): Expr[List[A]] = ListPrepend(elem, x)
    def append(elem: Expr[A]): Expr[List[A]] = ListAppend(x, elem)

  extension [A](x: Expr[List[A]] ) (using ResultTag[List[A]] )
    def contains(elem: Expr[A]): Expr[Boolean] = ListContains(x, elem)
    def length: Expr[Int] = ListLength(x)

  // Aggregations
  def sum(x: Expr[Int]): AggregationExpr[Int] = AggregationExpr.Sum(x) // TODO: require summable type?

  @targetName("doubleSum")
  def sum(x: Expr[Double]): AggregationExpr[Double] = AggregationExpr.Sum(x) // TODO: require summable type?

  def avg[T: ResultTag](x: Expr[T]): AggregationExpr[T] = AggregationExpr.Avg(x)

  @targetName("doubleAvg")
  def avg(x: Expr[Double]): AggregationExpr[Double] = AggregationExpr.Avg(x)

  def max[T: ResultTag](x: Expr[T]): AggregationExpr[T] = AggregationExpr.Max(x)

  def min[T: ResultTag](x: Expr[T]): AggregationExpr[T] = AggregationExpr.Min(x)

  def count(x: Expr[Int]): AggregationExpr[Int] = AggregationExpr.Count(x)
  @targetName("stringCnt")
  def count(x: Expr[String]): AggregationExpr[Int] = AggregationExpr.Count(x)
  def countAll: AggregationExpr[Int] = AggregationExpr.CountAll()

  // Note: All field names of constructors in the query language are prefixed with `$`
  // so that we don't accidentally pick a field name of a constructor class where we want
  // a name in the domain model instead.

  // Some sample constructors for Exprs
  case class Lt($x: Expr[Int], $y: Expr[Int]) extends Expr[Boolean]
  case class Lte($x: Expr[Int], $y: Expr[Int]) extends Expr[Boolean]
  case class Gt($x: Expr[Int], $y: Expr[Int]) extends Expr[Boolean]
  case class Gte($x: Expr[Int], $y: Expr[Int]) extends Expr[Boolean]
  case class GtDouble($x: Expr[Double], $y: Expr[Double]) extends Expr[Boolean]
  case class LtDouble($x: Expr[Double], $y: Expr[Double]) extends Expr[Boolean]
  case class LteDouble($x: Expr[Double], $y: Expr[Double]) extends Expr[Boolean]
  case class GteDouble($x: Expr[Double], $y: Expr[Double]) extends Expr[Boolean]
  case class GtDate($x: Expr[LocalDate], $y: Expr[LocalDate]) extends Expr[Boolean]
  case class LtDate($x: Expr[LocalDate], $y: Expr[LocalDate]) extends Expr[Boolean]
  case class LteDate($x: Expr[LocalDate], $y: Expr[LocalDate]) extends Expr[Boolean]
  case class GteDate($x: Expr[LocalDate], $y: Expr[LocalDate]) extends Expr[Boolean]

  case class Plus[T: Numeric]($x: Expr[T], $y: Expr[T])(using ResultTag[T]) extends Expr[T]
  case class Minus[T: Numeric]($x: Expr[T], $y: Expr[T])(using ResultTag[T]) extends Expr[T]
  case class Times[T: Numeric]($x: Expr[T], $y: Expr[T])(using ResultTag[T]) extends Expr[T]
  case class And($x: Expr[Boolean], $y: Expr[Boolean]) extends Expr[Boolean]
  case class Or($x: Expr[Boolean], $y: Expr[Boolean]) extends Expr[Boolean]
  case class Not($x: Expr[Boolean]) extends Expr[Boolean]

  case class Upper($x: Expr[String]) extends Expr[String]
  case class Lower($x: Expr[String]) extends Expr[String]

  case class ListExpr[A]($elements: List[Expr[A]])(using ResultTag[List[A]]) extends Expr[List[A]]
  extension [A, E <: Expr[A]](x: List[E])
    def toExpr(using ResultTag[List[A]]): ListExpr[A] = ListExpr(x)
  //  given Conversion[List[A], ListExpr[A]] = ListExpr(_)

  case class ListPrepend[A]($x: Expr[A], $list: Expr[List[A]])(using ResultTag[List[A]]) extends Expr[List[A]]
  case class ListAppend[A]($list: Expr[List[A]], $x: Expr[A])(using ResultTag[List[A]]) extends Expr[List[A]]
  case class ListContains[A]($list: Expr[List[A]], $x: Expr[A])(using ResultTag[Boolean]) extends Expr[Boolean]
  case class ListLength[A]($list: Expr[List[A]])(using ResultTag[Int]) extends Expr[Int]

  // So far Select is weakly typed, so `selectDynamic` is easy to implement.
  // Todo: Make it strongly typed like the other cases
  case class Select[A: ResultTag]($x: Expr[A], $name: String) extends Expr[A]

//  case class Single[S <: String, A]($x: Expr[A])(using ResultTag[NamedTuple[S *: EmptyTuple, A *: EmptyTuple]]) extends Expr[NamedTuple[S *: EmptyTuple, A *: EmptyTuple]]

  case class Concat[A <: AnyNamedTuple, B <: AnyNamedTuple]($x: Expr[A], $y: Expr[B])(using ResultTag[NamedTuple.Concat[A, B]]) extends Expr[NamedTuple.Concat[A, B]]

  case class Project[A <: AnyNamedTuple]($a: A)(using ResultTag[NamedTuple.Map[A, StripExpr]]) extends Expr[NamedTuple.Map[A, StripExpr]]

  type StripExpr[E] = E match
    case Expr[b] => b
    case AggregationExpr[b] => b

  // Also weakly typed in the arguments since these two classes model universal equality */
  case class Eq($x: Expr[?], $y: Expr[?]) extends Expr[Boolean]
  case class Ne($x: Expr[?], $y: Expr[?]) extends Expr[Boolean]

  // Expressions resulting from queries
  // Cannot use Contains with an aggregation
  case class Contains[A]($this: Query[A], $other: Expr[A]) extends Expr[Boolean]
  case class IsEmpty[A]($this: Query[A]) extends Expr[Boolean]
  case class NonEmpty[A]($this: Query[A]) extends Expr[Boolean]

  /** References are placeholders for parameters */
  private var refCount = 0 // TODO: do we want to recount from 0 for each query?
  private var exprRefCount = 0

  // References to relations
  case class Ref[A: ResultTag](idx: Int = -1) extends Expr[A]:
    private val $id = refCount
    refCount += 1
    val idxStr = if idx == -1 then "" else s"_$idx"
    def stringRef() = s"ref${$id}$idxStr"
    override def toString: String = s"Ref[${stringRef()}]$idxStr"

  /** The internal representation of a function `A => B`
   * Query languages are usually first-order, so Fun is not an Expr
   */
  case class Fun[A, B]($param: Ref[A], $body: B)

  /** Literals are type-specific, tailored to the types that the DB supports */
  case class IntLit($value: Int) extends Expr[Int]
  /** Scala values can be lifted into literals by conversions */
  given Conversion[Int, IntLit] = IntLit(_)

  case class StringLit($value: String) extends Expr[String]
  given Conversion[String, StringLit] = StringLit(_)

  case class DoubleLit($value: Double) extends Expr[Double]
  given Conversion[Double, DoubleLit] = DoubleLit(_)

  case class DateLit($value: LocalDate) extends Expr[LocalDate]
  given Conversion[LocalDate, DateLit] = DateLit(_)

  case class BooleanLit($value: Boolean) extends Expr[Boolean]
  //  given Conversion[Boolean, BooleanLit] = BooleanLit(_)

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
  case class RefExpr[A: ResultTag]() extends Expr[A]:
    private val id = exprRefCount
    exprRefCount += 1
    def stringRef() = s"exprRef$id"
    override def toString: String = s"ExprRef[${stringRef()}]"

  case class AbstractedExpr[A, B]($param: RefExpr[A], $body: Expr[B]):
    def apply(exprArg: Expr[A]): Expr[B] =
      substitute($body, $param, exprArg)
    private def substitute[C](expr: Expr[B],
                              formalP: RefExpr[A],
                              actualP: Expr[A]): Expr[B] = ???
  type IsTupleOfExpr[A <: AnyNamedTuple] = Tuple.Union[NamedTuple.DropNames[A]] <:< Expr[?]

  /** Explicit conversion from
   *      (name_1: Expr[T_1], ..., name_n: Expr[T_n])
   *  to
   *      Expr[(name_1: T_1, ..., name_n: T_n)]
   */
//  extension [A <: AnyNamedTuple : IsTupleOfExpr](using ev: IsTupleOfNonRestricted[A] =:= true)(x: A)
//    def toRow(using ResultTag[NamedTuple.Map[A, StripExpr]]): Project[A, NonRestricted] = Project(x)
//  extension [A <: AnyNamedTuple : IsTupleOfExpr](using ev: IsTupleOfRestricted[A] =:= true)(x: A)
//    def toRow(using ResultTag[NamedTuple.Map[A, StripExpr]]): Project[A, Restricted] = Project(x)
  extension [A <: AnyNamedTuple : IsTupleOfExpr](x: A)
    def toRow(using ResultTag[NamedTuple.Map[A, StripExpr]]): Project[A] = Project(x)

// TODO: use NamedTuple.from to convert case classes to named tuples before using concat
  extension [A <: AnyNamedTuple](x: Expr[A])
    def concat[B <: AnyNamedTuple](other: Expr[B])(using ResultTag[NamedTuple.Concat[A, B]]): Expr[NamedTuple.Concat[A, B]] = Concat(x, other)

  /** Same as _.toRow, as an implicit conversion */
//  given [A <: AnyNamedTuple : IsTupleOfExpr](using ResultTag[NamedTuple.Map[A, StripExpr]]): Conversion[A, Expr.Project[A]] = Expr.Project(_)

end Expr
