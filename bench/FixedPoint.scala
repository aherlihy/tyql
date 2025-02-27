package tyql.bench
import scala.collection.mutable
import scalasql.{Table as ScalaSQLTable, DbApi, query, Expr}
import scalasql.dialects.PostgresDialect.*
import scala.annotation.experimental

type constant = String | Int | Double
@experimental
object FixedPointQuery {
  val database = mutable.Map[String, Seq[constant]]()

  export ScalaSQLFixedPointQuery.*

  @annotation.tailrec
  final def fix[P](set: Boolean)(bases: Seq[P], acc: Seq[P])(fns: Seq[P] => Seq[P]): Seq[P] =
    if (Thread.currentThread().isInterrupted) throw new Exception(s"timed out")
    val next = fns(bases)
    if (next.toSet.subsetOf(acc.toSet))
      if (set) then (acc ++ bases).distinct else acc ++ bases
    else
      val res = if (set) then (acc ++ bases).distinct else acc ++ bases
      val nextClean = next.filterNot(n => res.contains(n))
      fix(set)(nextClean, res)(fns)

  @annotation.tailrec
  final def multiFix[T <: Tuple, S <: Seq[?]](set: Boolean, targetIdx: Int = 0)(bases: T, acc: T)(fns: (T, T) => T): T =
    if (Thread.currentThread().isInterrupted) throw new Exception(s"timed out")
    val next = fns(bases, acc)

    val nextA = next.toList.asInstanceOf[List[S]]
    val basesA = bases.toList.asInstanceOf[List[S]]
    val accA = acc.toList.asInstanceOf[List[S]]


    val cmp = nextA.zip(accA).map(
      (n, a) => n.toSet.subsetOf(a.toSet)
    ).forall(b => b)
    if (cmp)
      val combo = accA.zip(basesA).map((a: S, b: S) => if (set) then (a ++ b).distinct else a ++ b)
      val res = Tuple.fromArray(combo.toArray).asInstanceOf[T]
      res
    else
      val combo = accA.zip(basesA).map((a: S, b: S) => if (set) then (a ++ b).distinct else a ++ b)
      val newAcc = Tuple.fromArray(combo.toArray).asInstanceOf[T]
      val nextClean = nextA.zip(combo).map((n, r) => n.filterNot(n => r.contains(n)))
      val newNext = Tuple.fromArray(nextClean.toArray).asInstanceOf[T]
      multiFix(set)(newNext, newAcc)(fns)

}
