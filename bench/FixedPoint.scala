package tyql.bench
import scala.collection.mutable
import scalasql.{Table as ScalaSQLTable, DbApi, query, Expr}
import scalasql.dialects.PostgresDialect.*

type constant = String | Int | Double
object FixedPointQuery {
  val database = mutable.Map[String, Seq[constant]]()
  @annotation.tailrec
  final def fix[P](set: Boolean)(bases: Seq[P], acc: Seq[P])(fns: Seq[P] => Seq[P]): Seq[P] =
    val next = fns(bases)
    if (next == bases)
      if (set) then (acc ++ bases).distinct else acc ++ bases
    else
      val res = if (set) then (acc ++ bases).distinct else acc ++ bases
      fix(set)(next, res)(fns)

  final def scalaSQLFix[P[_[_]]]
    (bases: ScalaSQLTable[P], acc: ScalaSQLTable[P])
    (fns: ScalaSQLTable[P] => ScalaSQLTable[P])
    (cmp: ScalaSQLTable[P] => Boolean)
    (init: () => Unit)
  : ScalaSQLTable[P] =

    val delta = fns(bases)

    val isEmpty = cmp(delta)
    if (isEmpty)
      acc
    else
      init()
      scalaSQLFix(bases, acc)(fns)(cmp)(init)

  final def scalaSQLSemiNaive[Q, T >: Tuple, P[_[_]]](set: Boolean)
                                                     (db: DbApi, delta: ScalaSQLTable[P], derived: ScalaSQLTable[P], tmp: ScalaSQLTable[P])
                                                     (toTuple: P[Expr] => Tuple)
                                                     (initBase: () => query.Select[T, Q])
                                                     (initRecur: ScalaSQLTable[P] => query.Select[T, Q])
  : Unit = { //ScalaSQLTable[P] =

    db.run(delta.delete(_ => true))
    db.run(derived.delete(_ => true))
    db.run(tmp.delete(_ => true))

    db.run(delta.insert.select(
      toTuple,
      initBase()
    ))
    db.run(derived.insert.select(
      toTuple,
      initBase()
    ))

    val cmp: ScalaSQLTable[P] => Boolean = delta => db.run(delta.select).isEmpty

    val fixFn: ScalaSQLTable[P] => ScalaSQLTable[P] = recur => {
      val query = initRecur(recur)

      db.run(tmp.delete(_ => true))
      db.run(tmp.insert.select( // need tmp because reads from delta
        toTuple,
        query
      ))
      db.run(delta.delete(_ => true))
      val setStr = if (set) then " DISTINCT" else ""
      db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(delta)} (SELECT$setStr * FROM ${ScalaSQLTable.name(tmp)})")
      delta
    }

    val init: () => Unit = () => {
      if (set)
        db.run(tmp.delete(_ => true))
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(tmp)} (SELECT * FROM ${ScalaSQLTable.name(delta)} UNION SELECT * FROM ${ScalaSQLTable.name(derived)})")
        db.run(derived.delete(_ => true))
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(derived)} (SELECT * FROM ${ScalaSQLTable.name(tmp)})")
      else
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(derived)} (SELECT * FROM ${ScalaSQLTable.name(delta)})")
    }

    scalaSQLFix(delta, derived)(fixFn)(cmp)(init)
    init()
  }

}
