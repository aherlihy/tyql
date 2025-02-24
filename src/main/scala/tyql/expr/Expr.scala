package tyql

import scala.annotation.targetName
import language.experimental.namedTuples
import NamedTuple.{AnyNamedTuple, NamedTuple}
import scala.deriving.*
import scala.compiletime.{erasedValue, summonInline}

// TODO: probably seal
trait ExprShape
class ScalarExpr extends ExprShape
class NonScalarExpr extends ExprShape

type CalculatedShape[S1 <: ExprShape, S2 <: ExprShape] <: ExprShape = S2 match
  case ScalarExpr => S2
  case NonScalarExpr => S1

trait ConstructorFreedom
class RestrictedConstructors extends ConstructorFreedom
class NonRestrictedConstructors extends ConstructorFreedom

type CalculatedCF[S1 <: ConstructorFreedom, S2 <: ConstructorFreedom] <: ConstructorFreedom = S2 match
  case RestrictedConstructors => S2
  case NonRestrictedConstructors => S1
/** The type of expressions in the query language */
trait Expr[Result, Shape <: ExprShape, CF <: ConstructorFreedom](using val tag: ResultTag[Result]) extends Selectable:
  /** This type is used to support selection with any of the field names
   *  defined by Fields.
   */
  type Fields = NamedTuple.Map[NamedTuple.From[Result], [T] =>> Expr[T, Shape, CF]]

 /** A selection of a field name defined by Fields is implemented by `selectDynamic`.
   *  The implementation will add a cast to the right Expr type corresponding
   *  to the field type.
   */
  def selectDynamic(fieldName: String) = Expr.Select(this, fieldName)

  /** Member methods to implement universal equality on Expr level. */
  @targetName("eqNonScalarRestricted")
  def ==(other: Expr[?, NonScalarExpr, RestrictedConstructors]): Expr[Boolean, Shape, RestrictedConstructors] = Expr.Eq[Shape, NonScalarExpr, CF, RestrictedConstructors](this, other)
  @targetName("eqNonScalarNonRestricted")
  def ==(other: Expr[?, NonScalarExpr, NonRestrictedConstructors]): Expr[Boolean, Shape, CF] = Expr.Eq[Shape, NonScalarExpr, CF, NonRestrictedConstructors](this, other)
  @targetName("eqScalarRestricted")
  def ==(other: Expr[?, ScalarExpr, RestrictedConstructors]): Expr[Boolean, ScalarExpr, RestrictedConstructors] = Expr.Eq[Shape, ScalarExpr, CF, RestrictedConstructors](this, other)
  @targetName("eqScalarNonRestricted")
  def ==(other: Expr[?, ScalarExpr, NonRestrictedConstructors]): Expr[Boolean, ScalarExpr, CF] = Expr.Eq[Shape, ScalarExpr, CF, NonRestrictedConstructors](this, other)
//  def == [S <: ScalarExpr](other: Expr[?, S]): Expr[Boolean, CalculatedShape[Shape, S]] = Expr.Eq(this, other)
  def ==(other: String): Expr[Boolean, Shape, CF] = Expr.Eq(this, Expr.StringLit(other))
  def ==(other: Int): Expr[Boolean, Shape, CF] = Expr.Eq(this, Expr.IntLit(other))
  def ==(other: Boolean): Expr[Boolean, Shape, CF] = Expr.Eq(this, Expr.BooleanLit(other))

  @targetName("neqNonScalarRestricted")
  def != (other: Expr[?, NonScalarExpr, RestrictedConstructors]): Expr[Boolean, Shape, RestrictedConstructors] = Expr.Ne[Shape, NonScalarExpr, CF, RestrictedConstructors](this, other)
  @targetName("neqNonScalarNonRestricted")
  def != (other: Expr[?, NonScalarExpr, NonRestrictedConstructors]): Expr[Boolean, Shape, CF] = Expr.Ne[Shape, NonScalarExpr, CF, NonRestrictedConstructors](this, other)
  @targetName("neqScalarRestricted")
  def != (other: Expr[?, ScalarExpr, RestrictedConstructors]): Expr[Boolean, ScalarExpr, RestrictedConstructors] = Expr.Ne[Shape, ScalarExpr, CF, RestrictedConstructors](this, other)
  @targetName("neqScalarNonRestricted")
  def != (other: Expr[?, ScalarExpr, NonRestrictedConstructors]): Expr[Boolean, ScalarExpr, CF] = Expr.Ne[Shape, ScalarExpr, CF, NonRestrictedConstructors](this, other)

object Expr:
  /** Sample extension methods for individual types */
  extension [S1 <: ExprShape, CF1 <: ConstructorFreedom](x: Expr[Int, S1, CF1])
    def >[S2 <: ExprShape, CF2 <: ConstructorFreedom] (y: Expr[Int, S2, CF2]): Expr[Boolean, CalculatedShape[S1, S2], CalculatedCF[CF1, CF2]] = Gt(x, y)
    def >(y: Int): Expr[Boolean, S1, CF1] = Gt[S1, NonScalarExpr, CF1, NonRestrictedConstructors](x, IntLit(y))
    def <[S2 <: ExprShape, CF2 <: ConstructorFreedom] (y: Expr[Int, S2, CF2]): Expr[Boolean, CalculatedShape[S1, S2], CalculatedCF[CF1, CF2]] = Lt(x, y)
    def <(y: Int): Expr[Boolean, S1, CF1] = Lt[S1, NonScalarExpr, CF1, NonRestrictedConstructors](x, IntLit(y))
    def <=[S2 <: ExprShape, CF2 <: ConstructorFreedom] (y: Expr[Int, S2, CF2]): Expr[Boolean, CalculatedShape[S1, S2], CalculatedCF[CF1, CF2]] = Lte(x, y)

    // (NonRestricted|Restricted) op NonRestricted
//    def +[S2 <: ExprShape](y: Expr[Int, S2, NonRestricted]): Expr[Int, CalculatedShape[S1, S2], CF1] = Plus(x, y)
//    def *[S2 <: ExprShape](y: Expr[Int, S2, NonRestricted]): Expr[Int, CalculatedShape[S1, S2], CF1] = Times(x, y)

  // TODO: write for numerical
  extension [S1 <: ExprShape, CF1 <: ConstructorFreedom](x: Expr[Double, S1, CF1])
    @targetName("gtDoubleExpr")
    def >[S2 <: ExprShape, CF2 <: ConstructorFreedom](y: Expr[Double, S2, CF2]): Expr[Boolean, CalculatedShape[S1, S2], CalculatedCF[CF1, CF2]] = // allowed for all cf
      GtDouble(x, y)
    @targetName("gtDoubleLit")
    def >(y: Double): Expr[Boolean, S1, CF1] = GtDouble[S1, NonScalarExpr, CF1, NonRestrictedConstructors](x, DoubleLit(y))
    def <(y: Double): Expr[Boolean, S1, CF1] = LtDouble[S1, NonScalarExpr, CF1, NonRestrictedConstructors](x, DoubleLit(y))

    def *(y: Double): Expr[Double, S1, CF1] = Times[S1, NonScalarExpr, CF1, NonRestrictedConstructors, Double](x, DoubleLit(y))

    // (NonRestricted|Restricted) op NonRestricted
//    @targetName("addDoubleToNonRestricted")
//    def +[S2 <: ExprShape](y: Expr[Double, S2, NonRestricted]): Expr[Double, CalculatedShape[S1, S2], CF1] = Plus(x, y)
//    @targetName("multiplyDoubleToNonRestricted")
//    def *[S2 <: ExprShape](y: Expr[Double, S2, NonRestricted]): Expr[Double, CalculatedShape[S1, S2], CF1] = Times(x, y)

  extension [S1 <: ExprShape, CF1 <: ConstructorFreedom](x: Expr[Boolean, S1, CF1])
    def &&[S2 <: ExprShape, CF2 <: ConstructorFreedom] (y: Expr[Boolean, S2, CF2]): Expr[Boolean, CalculatedShape[S1, S2], CalculatedCF[CF1, CF2]] = And(x, y)
    def ||[S2 <: ExprShape, CF2 <: ConstructorFreedom] (y: Expr[Boolean, S2, CF2]): Expr[Boolean, CalculatedShape[S1, S2], CalculatedCF[CF1, CF2]] = Or(x, y)
    def unary_! = Not(x)

  extension [S1 <: ExprShape, CF1 <: ConstructorFreedom](x: Expr[String, S1, CF1])
    def toLowerCase: Expr[String, S1, CF1] = Expr.Lower(x)
    def toUpperCase: Expr[String, S1, CF1] = Expr.Upper(x)

  // NonRestricted op NonRestricted
  extension [S1 <: ExprShape](x: Expr[Double, S1, NonRestrictedConstructors])
    @targetName("addDoubleToNonRestricted")
    def +[S2 <: ExprShape](y: Expr[Double, S2, NonRestrictedConstructors]): Expr[Double, CalculatedShape[S1, S2], NonRestrictedConstructors] = Plus(x, y)
    @targetName("multipleDoubleToNonRestricted")
    def *[S2 <: ExprShape](y: Expr[Double, S2, NonRestrictedConstructors]): Expr[Double, CalculatedShape[S1, S2], NonRestrictedConstructors] = Times(x, y)
  extension [S1 <: ExprShape](x: Expr[Int, S1, NonRestrictedConstructors])
    @targetName("addDoubleToNonRestrictedInt")
    def +[S2 <: ExprShape](y: Expr[Int, S2, NonRestrictedConstructors]): Expr[Int, CalculatedShape[S1, S2], NonRestrictedConstructors] = Plus(x, y)
    @targetName("multipleDoubleToNonRestrictedInt")
    def *[S2 <: ExprShape](y: Expr[Int, S2, NonRestrictedConstructors]): Expr[Int, CalculatedShape[S1, S2], NonRestrictedConstructors] = Times(x, y)

  extension [A](x: Expr[List[A], NonScalarExpr, NonRestrictedConstructors])(using ResultTag[List[A]])
    def prepend(elem: Expr[A, NonScalarExpr, NonRestrictedConstructors]): Expr[List[A], NonScalarExpr, NonRestrictedConstructors] = ListPrepend(elem, x)
    def append(elem: Expr[A, NonScalarExpr, NonRestrictedConstructors]): Expr[List[A], NonScalarExpr, NonRestrictedConstructors] = ListAppend(x, elem)

  extension [A, CF <: ConstructorFreedom](x: Expr[List[A], NonScalarExpr, CF] ) (using ResultTag[List[A]] )
    def contains(elem: Expr[A, NonScalarExpr, ?]): Expr[Boolean, NonScalarExpr, CF] = ListContains(x, elem)
    def length: Expr[Int, NonScalarExpr, CF] = ListLength(x)

  // Aggregations
  def sum(x: Expr[Int, ?, NonRestrictedConstructors]): AggregationExpr[Int] = AggregationExpr.Sum(x) // TODO: require summable type?

  @targetName("doubleSum")
  def sum(x: Expr[Double, ?, NonRestrictedConstructors]): AggregationExpr[Double] = AggregationExpr.Sum(x) // TODO: require summable type?

  def avg[T: ResultTag](x: Expr[T, ?, NonRestrictedConstructors]): AggregationExpr[T] = AggregationExpr.Avg(x)

  @targetName("doubleAvg")
  def avg(x: Expr[Double, ?, NonRestrictedConstructors]): AggregationExpr[Double] = AggregationExpr.Avg(x)

  def max[T: ResultTag](x: Expr[T, ?, NonRestrictedConstructors]): AggregationExpr[T] = AggregationExpr.Max(x)

  def min[T: ResultTag](x: Expr[T, ?, NonRestrictedConstructors]): AggregationExpr[T] = AggregationExpr.Min(x)

  def count(x: Expr[Int, ?, NonRestrictedConstructors]): AggregationExpr[Int] = AggregationExpr.Count(x)
  @targetName("stringCnt")
  def count(x: Expr[String, ?, NonRestrictedConstructors]): AggregationExpr[Int] = AggregationExpr.Count(x)

  // Note: All field names of constructors in the query language are prefixed with `$`
  // so that we don't accidentally pick a field name of a constructor class where we want
  // a name in the domain model instead.

  // Some sample constructors for Exprs
  case class Lt[S1 <: ExprShape, S2 <: ExprShape, CF1 <: ConstructorFreedom, CF2 <: ConstructorFreedom]($x: Expr[Int, S1, CF1], $y: Expr[Int, S2, CF2]) extends Expr[Boolean, CalculatedShape[S1, S2], CalculatedCF[CF1, CF2]]
  case class Lte[S1 <: ExprShape, S2 <: ExprShape, CF1 <: ConstructorFreedom, CF2 <: ConstructorFreedom]($x: Expr[Int, S1, CF1], $y: Expr[Int, S2, CF2]) extends Expr[Boolean, CalculatedShape[S1, S2], CalculatedCF[CF1, CF2]]
  case class Gt[S1 <: ExprShape, S2 <: ExprShape, CF1 <: ConstructorFreedom, CF2 <: ConstructorFreedom]($x: Expr[Int, S1, CF1], $y: Expr[Int, S2, CF2]) extends Expr[Boolean, CalculatedShape[S1, S2], CalculatedCF[CF1, CF2]]
  case class GtDouble[S1 <: ExprShape, S2 <: ExprShape, CF1 <: ConstructorFreedom, CF2 <: ConstructorFreedom]($x: Expr[Double, S1, CF1], $y: Expr[Double, S2, CF2]) extends Expr[Boolean, CalculatedShape[S1, S2], CalculatedCF[CF1, CF2]]
  case class LtDouble[S1 <: ExprShape, S2 <: ExprShape, CF1 <: ConstructorFreedom, CF2 <: ConstructorFreedom]($x: Expr[Double, S1, CF1], $y: Expr[Double, S2, CF2]) extends Expr[Boolean, CalculatedShape[S1, S2], CalculatedCF[CF1, CF2]]

  case class Plus[S1 <: ExprShape, S2 <: ExprShape, CF1 <: ConstructorFreedom, CF2 <: ConstructorFreedom, T: Numeric]($x: Expr[T, S1, CF1], $y: Expr[T, S2, CF2])(using ResultTag[T]) extends Expr[T, CalculatedShape[S1, S2],  CalculatedCF[CF1, CF2]]
  case class Times[S1 <: ExprShape, S2 <: ExprShape, CF1 <: ConstructorFreedom, CF2 <: ConstructorFreedom, T: Numeric]($x: Expr[T, S1, CF1], $y: Expr[T, S2, CF2])(using ResultTag[T]) extends Expr[T, CalculatedShape[S1, S2],  CalculatedCF[CF1, CF2]]
  case class And[S1 <: ExprShape, S2 <: ExprShape, CF1 <: ConstructorFreedom, CF2 <: ConstructorFreedom]($x: Expr[Boolean, S1, CF1], $y: Expr[Boolean, S2, CF2]) extends Expr[Boolean, CalculatedShape[S1, S2],  CalculatedCF[CF1, CF2]]
  case class Or[S1 <: ExprShape, S2 <: ExprShape, CF1 <: ConstructorFreedom, CF2 <: ConstructorFreedom]($x: Expr[Boolean, S1, CF1], $y: Expr[Boolean, S2, CF2]) extends Expr[Boolean, CalculatedShape[S1, S2],  CalculatedCF[CF1, CF2]]
  case class Not[S1 <: ExprShape, CF1 <: ConstructorFreedom]($x: Expr[Boolean, S1, CF1]) extends Expr[Boolean, S1, CF1]

  case class Upper[S <: ExprShape, CF1 <: ConstructorFreedom]($x: Expr[String, S, CF1]) extends Expr[String, S, CF1]
  case class Lower[S <: ExprShape, CF1 <: ConstructorFreedom]($x: Expr[String, S, CF1]) extends Expr[String, S, CF1]

  case class ListExpr[A]($elements: List[Expr[A, NonScalarExpr, NonRestrictedConstructors]])(using ResultTag[List[A]]) extends Expr[List[A], NonScalarExpr, NonRestrictedConstructors]
  extension [A, E <: Expr[A, NonScalarExpr, NonRestrictedConstructors]](x: List[E])
    def toExpr(using ResultTag[List[A]]): ListExpr[A] = ListExpr(x)
  //  given Conversion[List[A], ListExpr[A]] = ListExpr(_)

  case class ListPrepend[A]($x: Expr[A, NonScalarExpr, NonRestrictedConstructors], $list: Expr[List[A], NonScalarExpr, NonRestrictedConstructors])(using ResultTag[List[A]]) extends Expr[List[A], NonScalarExpr, NonRestrictedConstructors]
  case class ListAppend[A]($list: Expr[List[A], NonScalarExpr, NonRestrictedConstructors], $x: Expr[A, NonScalarExpr, NonRestrictedConstructors])(using ResultTag[List[A]]) extends Expr[List[A], NonScalarExpr, NonRestrictedConstructors]
  case class ListContains[A, CF1 <: ConstructorFreedom]($list: Expr[List[A], NonScalarExpr, CF1], $x: Expr[A, NonScalarExpr, ?])(using ResultTag[Boolean]) extends Expr[Boolean, NonScalarExpr, CF1]
  case class ListLength[A, CF1 <: ConstructorFreedom]($list: Expr[List[A], NonScalarExpr, CF1])(using ResultTag[Int]) extends Expr[Int, NonScalarExpr, CF1]

  // So far Select is weakly typed, so `selectDynamic` is easy to implement.
  // Todo: Make it strongly typed like the other cases
  case class Select[A: ResultTag, CF1 <: ConstructorFreedom]($x: Expr[A, ?, CF1], $name: String) extends Expr[A, NonScalarExpr, CF1]

//  case class Single[S <: String, A]($x: Expr[A])(using ResultTag[NamedTuple[S *: EmptyTuple, A *: EmptyTuple]]) extends Expr[NamedTuple[S *: EmptyTuple, A *: EmptyTuple]]

  case class Concat[A <: AnyNamedTuple, B <: AnyNamedTuple, S1 <: ExprShape, S2 <: ExprShape, CF1 <: ConstructorFreedom, CF2 <: ConstructorFreedom]($x: Expr[A, S1, CF1], $y: Expr[B, S2, CF2])(using ResultTag[NamedTuple.Concat[A, B]]) extends Expr[NamedTuple.Concat[A, B], CalculatedShape[S1, S2], CalculatedCF[CF1, CF2]]

  case class Project[A <: AnyNamedTuple, CF1 <: ConstructorFreedom]($a: A)(using ResultTag[NamedTuple.Map[A, StripExpr]]) extends Expr[NamedTuple.Map[A, StripExpr], NonScalarExpr, CF1]

  type StripExpr[E] = E match
    case Expr[b, s, cf] => b
    case AggregationExpr[b] => b

  // Also weakly typed in the arguments since these two classes model universal equality */
  case class Eq[S1 <: ExprShape, S2 <: ExprShape, CF1 <: ConstructorFreedom, CF2 <: ConstructorFreedom]($x: Expr[?, S1, CF1], $y: Expr[?, S2, CF2]) extends Expr[Boolean, CalculatedShape[S1, S2], CalculatedCF[CF1, CF2]]
  case class Ne[S1 <: ExprShape, S2 <: ExprShape, CF1 <: ConstructorFreedom, CF2 <: ConstructorFreedom]($x: Expr[?, S1, CF1], $y: Expr[?, S2, CF2]) extends Expr[Boolean, CalculatedShape[S1, S2], CalculatedCF[CF1, CF2]]

  // Expressions resulting from queries
  // Cannot use Contains with an aggregation
  case class Contains[A, CF1 <: ConstructorFreedom]($this: Query[A, ?], $other: Expr[A, NonScalarExpr, CF1]) extends Expr[Boolean, NonScalarExpr, CF1]
  case class IsEmpty[A, CF1 <: ConstructorFreedom]($this: Query[A, ?]) extends Expr[Boolean, NonScalarExpr, CF1]
  case class NonEmpty[A,  CF1 <: ConstructorFreedom]($this: Query[A, ?]) extends Expr[Boolean, NonScalarExpr, CF1]

  /** References are placeholders for parameters */
  private var refCount = 0 // TODO: do we want to recount from 0 for each query?
  private var exprRefCount = 0

  // References to relations
  case class Ref[A: ResultTag, S <: ExprShape, CF <: ConstructorFreedom](idx: Int = -1) extends Expr[A, S, CF]:
    private val $id = refCount
    refCount += 1
    val idxStr = if idx == -1 then "" else s"_$idx"
    def stringRef() = s"ref${$id}$idxStr"
    override def toString: String = s"Ref[${stringRef()}]$idxStr"

  /** The internal representation of a function `A => B`
   * Query languages are usually first-order, so Fun is not an Expr
   */
  case class Fun[A, B, S <: ExprShape, CF <: ConstructorFreedom]($param: Ref[A, S, CF], $body: B)

  /** Literals are type-specific, tailored to the types that the DB supports */
  case class IntLit($value: Int) extends Expr[Int, NonScalarExpr, NonRestrictedConstructors]
  /** Scala values can be lifted into literals by conversions */
  given Conversion[Int, IntLit] = IntLit(_)

  case class StringLit($value: String) extends Expr[String, NonScalarExpr, NonRestrictedConstructors]
  given Conversion[String, StringLit] = StringLit(_)

  case class DoubleLit($value: Double) extends Expr[Double, NonScalarExpr, NonRestrictedConstructors]
  given Conversion[Double, DoubleLit] = DoubleLit(_)

  case class BooleanLit($value: Boolean) extends Expr[Boolean, NonScalarExpr, NonRestrictedConstructors]
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
  case class RefExpr[A: ResultTag, S <: ExprShape, CF <: ConstructorFreedom]() extends Expr[A, S, CF]:
    private val id = exprRefCount
    exprRefCount += 1
    def stringRef() = s"exprRef$id"
    override def toString: String = s"ExprRef[${stringRef()}]"

  case class AbstractedExpr[A, B, S <: ExprShape, CF <: ConstructorFreedom]($param: RefExpr[A, S, CF], $body: Expr[B, S, CF]):
    def apply(exprArg: Expr[A, S, CF]): Expr[B, S, CF] =
      substitute($body, $param, exprArg)
    private def substitute[C](expr: Expr[B, S, CF],
                              formalP: RefExpr[A, S, CF],
                              actualP: Expr[A, S, CF]): Expr[B, S, CF] = ???
  type IsTupleOfExpr[A <: AnyNamedTuple] = Tuple.Union[NamedTuple.DropNames[A]] <:< Expr[?, NonScalarExpr, ?]

  type IsTupleOfNonRestricted[A <: AnyNamedTuple] = Tuple.Union[NamedTuple.DropNames[A]] <:< Expr[?, ?, NonRestrictedConstructors]
  type IsTupleOfRestricted[A <: AnyNamedTuple] = Tuple.Union[NamedTuple.DropNames[A]] <:< Expr[?, ?, RestrictedConstructors]

  /** Explicit conversion from
   *      (name_1: Expr[T_1], ..., name_n: Expr[T_n])
   *  to
   *      Expr[(name_1: T_1, ..., name_n: T_n)]
   */
//  extension [A <: AnyNamedTuple : IsTupleOfExpr](using ev: IsTupleOfNonRestricted[A] =:= true)(x: A)
//    def toRow(using ResultTag[NamedTuple.Map[A, StripExpr]]): Project[A, NonRestricted] = Project(x)
//  extension [A <: AnyNamedTuple : IsTupleOfExpr](using ev: IsTupleOfRestricted[A] =:= true)(x: A)
//    def toRow(using ResultTag[NamedTuple.Map[A, StripExpr]]): Project[A, Restricted] = Project(x)
  extension [A <: AnyNamedTuple : IsTupleOfExpr, CF <: ConstructorFreedom](x: A)
    def toRow(using ResultTag[NamedTuple.Map[A, StripExpr]]): Project[A, CF] = Project(x)

// TODO: use NamedTuple.from to convert case classes to named tuples before using concat
  extension [A <: AnyNamedTuple, S <: ExprShape, CF1 <: ConstructorFreedom](x: Expr[A, S, CF1])
    def concat[B <: AnyNamedTuple, S2 <: ExprShape, CF2 <: ConstructorFreedom](other: Expr[B, S2, CF2])(using ResultTag[NamedTuple.Concat[A, B]]): Expr[NamedTuple.Concat[A, B], CalculatedShape[S, S2], CalculatedCF[CF1, CF2]] = Concat(x, other)

  /** Same as _.toRow, as an implicit conversion */
//  given [A <: AnyNamedTuple : IsTupleOfExpr](using ResultTag[NamedTuple.Map[A, StripExpr]]): Conversion[A, Expr.Project[A]] = Expr.Project(_)

end Expr
