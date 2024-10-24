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

  final def multiFix[T <: Tuple, S <: Seq[?]](set: Boolean)(bases: T, acc: T)(fns: T => T): T =
    val next = fns(bases)

    val nextA = next.toArray
    val basesA = bases.toArray
    val accA = acc.toArray

    val combo = accA.zip(basesA).asInstanceOf[Array[(S, S)]].map((a: S, b: S) => if (set) then (a ++ b).distinct else a ++ b)
    val res = Tuple.fromArray(combo).asInstanceOf[T]

    if (nextA.zip(basesA).map((t: Tuple2[?, ?]) => t._1 == t._2).toList.forall(b => b))
      res
    else
      multiFix(set)(next, res)(fns)

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
  final def scalaSQLMultiFix[T](bases: T, acc: T)(fns: T => T)(cmp: T => Boolean)(init: () => Unit): T =
    val delta = fns(bases)

    val isEmpty = cmp(delta)
    if (isEmpty)
      acc
    else
      init()
      scalaSQLMultiFix(bases, acc)(fns)(cmp)(init)

//  type MapHK[Tup <: Tuple, F[_ <: AnyKind]] <: Tuple = Tup match {
//    case EmptyTuple => EmptyTuple
//    case h *: t => F[h] *: MapHK[t, F]
//  }
//  type ToScalaSQLTable[PT <: Tuple] = MapHK[PT, [P[_[_]]] =>> ScalaSQLTable[P]]
//  type ToToTuple[PT <: Tuple] = MapHK[PT, [P[_[_]]] =>> P[Expr] => Tuple]
//  type ToBase[TT <: Tuple, QT <: Tuple] = Tuple.Map[Tuple.Zip[TT, QT], [T] =>> T match
//    case (a, b) => query.Select[a, b]
//  ]
//  type ToRecur[ST <: Tuple, TT <: Tuple, QT <: Tuple] = Tuple.Map[
//    Tuple.Zip[ST, ToBase[TT, QT]],
//    [T] =>> T match
//      case (a, b) => a => b
//  ]

  // this is silly but higher kinded types are mega painful to abstract
  final def scalaSQLSemiNaive2[Q1, Q2, T1 >: Tuple, T2 >: Tuple, P1[_[_]], P2[_[_]]]
    (set: Boolean)
    (db: DbApi, delta: (ScalaSQLTable[P1], ScalaSQLTable[P2]), derived: (ScalaSQLTable[P1], ScalaSQLTable[P2]), tmp: (ScalaSQLTable[P1], ScalaSQLTable[P2]))
    (toTuple: (P1[Expr] => Tuple, P2[Expr] => Tuple))
    (initBase: () => (query.Select[T1, Q1], query.Select[T2, Q2]))
    (initRecur: (ScalaSQLTable[P1], ScalaSQLTable[P2]) => (query.Select[T1, Q1], query.Select[T2, Q2]))
  : Unit =

    db.run(delta._1.delete(_ => true))
    db.run(derived._1.delete(_ => true))
    db.run(tmp._1.delete(_ => true))

    db.run(delta._2.delete(_ => true))
    db.run(derived._2.delete(_ => true))
    db.run(tmp._2.delete(_ => true))

    val (base1, base2) = initBase()

    db.run(delta._1.insert.select(
      toTuple._1,
      base1
    ))
    db.run(derived._1.insert.select(
      toTuple._1,
      base1
    ))
    db.run(delta._2.insert.select(
      toTuple._2,
      base2
    ))

    db.run(derived._2.insert.select(
      toTuple._2,
      base2
    ))

//    println(s"delta1=${db.runRaw[(Int, String)](s"SELECT * FROM ${ScalaSQLTable.name(delta._1)}")}")
//    println(s"delta2=${db.runRaw[(Int, String)](s"SELECT * FROM ${ScalaSQLTable.name(delta._2)}")}")
//    println(s"derived1=${db.runRaw[(Int, String)](s"SELECT * FROM ${ScalaSQLTable.name(derived._1)}")}")
//    println(s"derived2=${db.runRaw[(Int, String)](s"SELECT * FROM ${ScalaSQLTable.name(derived._2)}")}")

    type st = (ScalaSQLTable[P1], ScalaSQLTable[P2])
    val cmp: st => Boolean = (delta1, delta2) => db.run(delta._1.select).isEmpty && db.run(delta._2.select).isEmpty

    val fixFn: st => (ScalaSQLTable[P1], ScalaSQLTable[P2]) = (recur1, recur2) => {
//      println(s"recur1=${ScalaSQLTable.name(recur1)}, recur2=${ScalaSQLTable.name(recur2)}")
      val (query1, query2) = initRecur(recur1, recur2)

      db.run(tmp._1.delete(_ => true))
      db.run(tmp._2.delete(_ => true))
      db.run(tmp._1.insert.select( // need tmp because reads from delta
        toTuple._1,
        query1
      ))
      db.run(tmp._2.insert.select( // need tmp because reads from delta
        toTuple._2,
        query2
      ))
//      println(s"tmp1=${db.runRaw[(Int, String)](s"SELECT * FROM ${ScalaSQLTable.name(tmp._1)}")}")
//      println(s"tmp2=${db.runRaw[(Int, String)](s"SELECT * FROM ${ScalaSQLTable.name(tmp._2)}")}")
      db.run(delta._1.delete(_ => true))
      db.run(delta._2.delete(_ => true))
      val setStr = if (set) then " DISTINCT" else ""
      db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(delta._1)} (SELECT$setStr * FROM ${ScalaSQLTable.name(tmp._1)})")
      db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(delta._2)} (SELECT$setStr * FROM ${ScalaSQLTable.name(tmp._2)})")
      delta
    }

    val init: () => Unit = () => {
      if (set)
        db.run(tmp._1.delete(_ => true))
        db.run(tmp._2.delete(_ => true))
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(tmp._1)} (SELECT * FROM ${ScalaSQLTable.name(delta._1)} UNION SELECT * FROM ${ScalaSQLTable.name(derived._1)})")
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(tmp._2)} (SELECT * FROM ${ScalaSQLTable.name(delta._2)} UNION SELECT * FROM ${ScalaSQLTable.name(derived._2)})")
        db.run(derived._1.delete(_ => true))
        db.run(derived._2.delete(_ => true))
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(derived._1)} (SELECT * FROM ${ScalaSQLTable.name(tmp._1)})")
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(derived._2)} (SELECT * FROM ${ScalaSQLTable.name(tmp._2)})")
      else
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(derived._1)} (SELECT * FROM ${ScalaSQLTable.name(delta._1)})")
        db.updateRaw(s"INSERT INTO ${ScalaSQLTable.name(derived._2)} (SELECT * FROM ${ScalaSQLTable.name(delta._2)})")
    }

    scalaSQLMultiFix(delta, derived)(fixFn)(cmp)(init)
    init()
}
