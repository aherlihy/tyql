package tyql.bench
import scala.collection.mutable
import scalasql.{Table as ScalaSQLTable, DbApi, query, Expr}
import scalasql.dialects.PostgresDialect.*
import scala.annotation.experimental

type constant = String | Int | Double
@experimental
object FixedPointQuery {
  val database = mutable.Map[String, Seq[constant]]()
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

    val cmp = nextA.zip(accA).map((n, a) => n.toSet.subsetOf(a.toSet)).forall(b => b)
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

  @annotation.tailrec
  final def scalaSQLFix[P[_[_]]]
    (bases: ScalaSQLTable[P], next: ScalaSQLTable[P], acc: ScalaSQLTable[P])
    (fns: (ScalaSQLTable[P], ScalaSQLTable[P]) => Unit)
    (cmp: (ScalaSQLTable[P], ScalaSQLTable[P]) => Boolean)
    (copyTo: (ScalaSQLTable[P], ScalaSQLTable[P], ScalaSQLTable[P]) => (ScalaSQLTable[P], ScalaSQLTable[P]))
    : ScalaSQLTable[P] =
    if (Thread.currentThread().isInterrupted) throw new Exception(s"timed out")

    fns(bases, next)

    val isEmpty = cmp(next, acc)
    if (isEmpty)
      acc
      copyTo(bases, acc, next)._1
    else
      val (newNext, newBase) = copyTo(bases, acc, next)
      scalaSQLFix(newBase, newNext, acc)(fns)(cmp)(copyTo)

  final def scalaSQLSemiNaive[Q, T >: Tuple, P[_[_]]]
    (set: Boolean)
    (ddb: DuckDBBackend, bases_db: ScalaSQLTable[P], next_db: ScalaSQLTable[P], acc_db: ScalaSQLTable[P])
    (toTuple: P[Expr] => Tuple)
    (initBase: () => query.Select[T, Q])
    (initRecur: ScalaSQLTable[P] => query.Select[T, Q])
    : Unit = {
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(bases_db)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next_db)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc_db)}")

    val str = db.renderSql(bases_db.insert.select(
      toTuple,
      initBase()
    ))
    ddb.runUpdate(str)

    val cmp: (ScalaSQLTable[P], ScalaSQLTable[P]) => Boolean = (next, acc) =>
      val newly = next.select.asInstanceOf[query.Select[T, Q]].except(acc.select.asInstanceOf[query.Select[T, Q]])
      val str = db.renderSql(newly)
      !ddb.runQuery(str).next()

    val fixFn: (ScalaSQLTable[P], ScalaSQLTable[P]) => Unit = (bases, next) => {
      val query = initRecur(bases)
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next)}")

      val sqlString = db.renderSql(next.insert.select(
        toTuple,
        query
      ))
      ddb.runUpdate(sqlString)
    }

    val copyTo: (ScalaSQLTable[P], ScalaSQLTable[P], ScalaSQLTable[P]) => (ScalaSQLTable[P], ScalaSQLTable[P]) =
      (bases, acc, next) => {
        val tmp = s"${ScalaSQLTable.name(bases)}_tmp"
        if (set)
          ddb.runUpdate(s"CREATE TABLE $tmp AS SELECT * FROM ${ScalaSQLTable.name(bases)} LIMIT 0")
          ddb.runUpdate(
            s"INSERT INTO $tmp (SELECT * FROM ${ScalaSQLTable.name(bases)} UNION SELECT * FROM ${ScalaSQLTable.name(acc)})"
          )
          ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc)}")
          ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc)} (SELECT * FROM $tmp)")
          ddb.runUpdate(s"DROP TABLE $tmp")
        else
          ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc)} (SELECT * FROM ${ScalaSQLTable.name(bases)})")

        ddb.runUpdate(s"CREATE TABLE $tmp AS SELECT * FROM ${ScalaSQLTable.name(bases)} LIMIT 0")
        ddb.runUpdate(
          s"INSERT INTO $tmp (SELECT * FROM ${ScalaSQLTable.name(next)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc)})"
        )
        ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next)}")
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(next)} (SELECT * FROM $tmp)")
        ddb.runUpdate(s"DROP TABLE $tmp")

        (bases, next)
      }

    scalaSQLFix(bases_db, next_db, acc_db)(fixFn)(cmp)(copyTo)
  }

  // Mutually recursive relations
  @annotation.tailrec
  final def scalaSQLMultiFix[T]
    (bases: T, next: T, acc: T)
    (fns: (T, T) => Unit)
    (cmp: (T, T) => Boolean)
    (copyTo: (T, T, T) => (T, T))
    : T = {
//    println(s"----- start multifix ------")
//    println(s"BASE=${ScalaSQLTable.name(bases.asInstanceOf[Tuple2[ScalaSQLTable[?], ScalaSQLTable[?]]]._1)}")
//    println(s"NEXT=${ScalaSQLTable.name(next.asInstanceOf[Tuple2[ScalaSQLTable[?], ScalaSQLTable[?]]]._1)}")
//    println(s"ACC=${ScalaSQLTable.name(acc.asInstanceOf[Tuple2[ScalaSQLTable[?], ScalaSQLTable[?]]]._1)}")
    if (Thread.currentThread().isInterrupted) throw new Exception(s"timed out")
    fns(bases, next)

    val isEmpty = cmp(next, acc)
    if (isEmpty)
      copyTo(bases, acc, next)
      acc
    else
      val (newNext, newBase) = copyTo(bases, acc, next)
      scalaSQLMultiFix(newBase, newNext, acc)(fns)(cmp)(copyTo)
  }

  // this is silly but higher kinded types are mega painful to abstract
  final def scalaSQLSemiNaiveTWO[Q1, Q2, T1 >: Tuple, T2 >: Tuple, P1[_[_]], P2[_[_]], Tables]
    (using Tables =:= (ScalaSQLTable[P1], ScalaSQLTable[P2]))
    (set: Boolean)
    (ddb: DuckDBBackend, bases_db: Tables, next_db: Tables, acc_db: Tables)
    (toTuple: (P1[Expr] => Tuple, P2[Expr] => Tuple))
    (initBase: () => (query.Select[T1, Q1], query.Select[T2, Q2]))
    (initRecur: Tables => (query.Select[T1, Q1], query.Select[T2, Q2]))
    : Unit = {
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection

    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(bases_db._1)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next_db._1)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc_db._1)}")

    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(bases_db._2)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next_db._2)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc_db._2)}")

    val (base1, base2) = initBase()

    val sqlString1 = "INSERT INTO " + ScalaSQLTable.name(bases_db._1) + " " + db.renderSql(base1)
    val sqlString2 = "INSERT INTO " + ScalaSQLTable.name(bases_db._2) + " " + db.renderSql(base2)

    ddb.runUpdate(sqlString1)
    ddb.runUpdate(sqlString2)

    def printTable(t: Tables, name: String): Unit =
      println(
        s"${name}1(${ScalaSQLTable.name(t._1)})=${db.runRaw[(String)](s"SELECT * FROM ${ScalaSQLTable.name(t._1)}")}"
      )
      println(
        s"${name}2(${ScalaSQLTable.name(t._2)})=${db.runRaw[(String, Int)](s"SELECT * FROM ${ScalaSQLTable.name(t._2)}")}"
      )

    val cmp: (Tables, Tables) => Boolean = (next, acc) => {
      val str1 = s"SELECT * FROM ${ScalaSQLTable.name(next._1)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc._1)}"
      val str2 = s"SELECT * FROM ${ScalaSQLTable.name(next._2)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc._2)}"

      val isEmpty1 = !ddb.runQuery(str1).next()
      val isEmpty2 = !ddb.runQuery(str2).next()
      isEmpty1 && isEmpty2
    }

    val fixFn: (Tables, Tables) => Unit = (base, next) => {
      val (query1, query2) = initRecur(base)
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._1)}")
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._2)}")
      val sqlString1 = db.renderSql(next._1.insert.select(
        toTuple._1,
        query1
      ))
      val sqlString2 = db.renderSql(next._2.insert.select(
        toTuple._2,
        query2
      ))
      ddb.runUpdate(sqlString1)
      ddb.runUpdate(sqlString2)
    }

    val copyInto: (Tables, Tables, Tables) => (Tables, Tables) = (base, acc, next) => {
      val tmp = (s"${ScalaSQLTable.name(base._1)}_tmp", s"${ScalaSQLTable.name(base._2)}_tmp")
      if (set)
        ddb.runUpdate(s"CREATE TABLE ${tmp._1} AS SELECT * FROM ${ScalaSQLTable.name(base._1)} LIMIT 0")
        ddb.runUpdate(
          s"INSERT INTO ${tmp._1} (SELECT * FROM ${ScalaSQLTable.name(base._1)} UNION SELECT * FROM ${ScalaSQLTable.name(acc._1)})"
        )

        ddb.runUpdate(s"CREATE TABLE ${tmp._2} AS SELECT * FROM ${ScalaSQLTable.name(base._2)} LIMIT 0")
        ddb.runUpdate(
          s"INSERT INTO ${tmp._2} (SELECT * FROM ${ScalaSQLTable.name(base._2)} UNION SELECT * FROM ${ScalaSQLTable.name(acc._2)})"
        )

        ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc._1)}")
        ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc._2)}")
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._1)} (SELECT * FROM ${tmp._1})")
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._2)} (SELECT * FROM ${tmp._2})")
        ddb.runUpdate(s"DROP TABLE ${tmp._1}")
        ddb.runUpdate(s"DROP TABLE ${tmp._2}")
      else
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._1)} (SELECT * FROM ${ScalaSQLTable.name(base._1)})")
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._2)} (SELECT * FROM ${ScalaSQLTable.name(base._2)})")

      ddb.runUpdate(s"CREATE TABLE ${tmp._1} AS SELECT * FROM ${ScalaSQLTable.name(base._1)} LIMIT 0")
      ddb.runUpdate(
        s"INSERT INTO ${tmp._1} (SELECT * FROM ${ScalaSQLTable.name(next._1)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc._1)})"
      )
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._1)}")
      ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(next._1)} (SELECT * FROM ${tmp._1})")
      ddb.runUpdate(s"DROP TABLE ${tmp._1}")

      ddb.runUpdate(s"CREATE TABLE ${tmp._2} AS SELECT * FROM ${ScalaSQLTable.name(base._2)} LIMIT 0")
      ddb.runUpdate(
        s"INSERT INTO ${tmp._2} (SELECT * FROM ${ScalaSQLTable.name(next._2)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc._2)})"
      )
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._2)}")
      ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(next._2)} (SELECT * FROM ${tmp._2})")
      ddb.runUpdate(s"DROP TABLE ${tmp._2}")

      (base, next)
    }

    scalaSQLMultiFix(bases_db, next_db, acc_db)(fixFn)(cmp)(copyInto)
  }

  final def scalaSQLSemiNaiveTHREE[
      Q1,
      Q2,
      Q3,
      T1 >: Tuple,
      T2 >: Tuple,
      T3 >: Tuple,
      P1[_[_]],
      P2[_[_]],
      P3[_[_]],
      Tables
  ]
    (using Tables =:= (ScalaSQLTable[P1], ScalaSQLTable[P2], ScalaSQLTable[P3]))
    (set: Boolean)
    (ddb: DuckDBBackend, bases_db: Tables, next_db: Tables, acc_db: Tables)
    (toTuple: (P1[Expr] => Tuple, P2[Expr] => Tuple, P3[Expr] => Tuple))
    (initBase: () => (query.Select[T1, Q1], query.Select[T2, Q2], query.Select[T3, Q3]))
    (initRecur: Tables => (query.Select[T1, Q1], query.Select[T2, Q2], query.Select[T3, Q3]))
    : Unit = {
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection

    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(bases_db._1)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next_db._1)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc_db._1)}")

    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(bases_db._2)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next_db._2)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc_db._2)}")

    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(bases_db._3)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next_db._3)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc_db._3)}")

    val (base1, base2, base3) = initBase()

    val sqlString1 = "INSERT INTO " + ScalaSQLTable.name(bases_db._1) + " " + db.renderSql(base1)
    val sqlString2 = "INSERT INTO " + ScalaSQLTable.name(bases_db._2) + " " + db.renderSql(base2)
    val sqlString3 = "INSERT INTO " + ScalaSQLTable.name(bases_db._3) + " " + db.renderSql(base3)

    ddb.runUpdate(sqlString1)
    ddb.runUpdate(sqlString2)
    ddb.runUpdate(sqlString3)

    def printTable(t: Tables, name: String): Unit =
      println(
        s"${name}1(${ScalaSQLTable.name(t._1)})=${db.runRaw[(String)](s"SELECT * FROM ${ScalaSQLTable.name(t._1)}")}"
      )
      println(
        s"${name}2(${ScalaSQLTable.name(t._2)})=${db.runRaw[(String, Int)](s"SELECT * FROM ${ScalaSQLTable.name(t._2)}")}"
      )
      println(
        s"${name}3(${ScalaSQLTable.name(t._3)})=${db.runRaw[(String, Int, Double)](s"SELECT * FROM ${ScalaSQLTable.name(t._3)}")}"
      )

    val cmp: (Tables, Tables) => Boolean = (next, acc) => {
      val str1 = s"SELECT * FROM ${ScalaSQLTable.name(next._1)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc._1)}"
      val str2 = s"SELECT * FROM ${ScalaSQLTable.name(next._2)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc._2)}"
      val str3 = s"SELECT * FROM ${ScalaSQLTable.name(next._3)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc._3)}"

      val isEmpty1 = !ddb.runQuery(str1).next()
      val isEmpty2 = !ddb.runQuery(str2).next()
      val isEmpty3 = !ddb.runQuery(str3).next()

      isEmpty1 && isEmpty2 && isEmpty3
    }

    val fixFn: (Tables, Tables) => Unit = (base, next) => {
      val (query1, query2, query3) = initRecur(base)
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._1)}")
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._2)}")
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._3)}")
      val sqlString1 = db.renderSql(next._1.insert.select(
        toTuple._1,
        query1
      ))
      val sqlString2 = db.renderSql(next._2.insert.select(
        toTuple._2,
        query2
      ))
      val sqlString3 = db.renderSql(next._3.insert.select(
        toTuple._3,
        query3
      ))
      ddb.runUpdate(sqlString1)
      ddb.runUpdate(sqlString2)
      ddb.runUpdate(sqlString3)
    }

    val copyInto: (Tables, Tables, Tables) => (Tables, Tables) = (base, acc, next) => {
      val tmp = (
        s"${ScalaSQLTable.name(base._1)}_tmp",
        s"${ScalaSQLTable.name(base._2)}_tmp",
        s"${ScalaSQLTable.name(base._3)}_tmp"
      )
      if (set)
        ddb.runUpdate(s"CREATE TABLE ${tmp._1} AS SELECT * FROM ${ScalaSQLTable.name(base._1)} LIMIT 0")
        ddb.runUpdate(
          s"INSERT INTO ${tmp._1} (SELECT * FROM ${ScalaSQLTable.name(base._1)} UNION SELECT * FROM ${ScalaSQLTable.name(acc._1)})"
        )

        ddb.runUpdate(s"CREATE TABLE ${tmp._2} AS SELECT * FROM ${ScalaSQLTable.name(base._2)} LIMIT 0")
        ddb.runUpdate(
          s"INSERT INTO ${tmp._2} (SELECT * FROM ${ScalaSQLTable.name(base._2)} UNION SELECT * FROM ${ScalaSQLTable.name(acc._2)})"
        )

        ddb.runUpdate(s"CREATE TABLE ${tmp._3} AS SELECT * FROM ${ScalaSQLTable.name(base._3)} LIMIT 0")
        ddb.runUpdate(
          s"INSERT INTO ${tmp._3} (SELECT * FROM ${ScalaSQLTable.name(base._3)} UNION SELECT * FROM ${ScalaSQLTable.name(acc._3)})"
        )

        ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc._1)}")
        ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc._2)}")
        ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc._3)}")
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._1)} (SELECT * FROM ${tmp._1})")
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._2)} (SELECT * FROM ${tmp._2})")
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._3)} (SELECT * FROM ${tmp._3})")
        ddb.runUpdate(s"DROP TABLE ${tmp._1}")
        ddb.runUpdate(s"DROP TABLE ${tmp._2}")
        ddb.runUpdate(s"DROP TABLE ${tmp._3}")
      else
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._1)} (SELECT * FROM ${ScalaSQLTable.name(base._1)})")
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._2)} (SELECT * FROM ${ScalaSQLTable.name(base._2)})")
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._3)} (SELECT * FROM ${ScalaSQLTable.name(base._3)})")

      ddb.runUpdate(s"CREATE TABLE ${tmp._1} AS SELECT * FROM ${ScalaSQLTable.name(base._1)} LIMIT 0")
      ddb.runUpdate(
        s"INSERT INTO ${tmp._1} (SELECT * FROM ${ScalaSQLTable.name(next._1)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc._1)})"
      )
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._1)}")
      ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(next._1)} (SELECT * FROM ${tmp._1})")
      ddb.runUpdate(s"DROP TABLE ${tmp._1}")

      ddb.runUpdate(s"CREATE TABLE ${tmp._2} AS SELECT * FROM ${ScalaSQLTable.name(base._2)} LIMIT 0")
      ddb.runUpdate(
        s"INSERT INTO ${tmp._2} (SELECT * FROM ${ScalaSQLTable.name(next._2)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc._2)})"
      )
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._2)}")
      ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(next._2)} (SELECT * FROM ${tmp._2})")
      ddb.runUpdate(s"DROP TABLE ${tmp._2}")

      ddb.runUpdate(s"CREATE TABLE ${tmp._3} AS SELECT * FROM ${ScalaSQLTable.name(base._3)} LIMIT 0")
      ddb.runUpdate(
        s"INSERT INTO ${tmp._3} (SELECT * FROM ${ScalaSQLTable.name(next._3)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc._3)})"
      )
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._3)}")
      ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(next._3)} (SELECT * FROM ${tmp._3})")
      ddb.runUpdate(s"DROP TABLE ${tmp._3}")

      (base, next)
    }

    scalaSQLMultiFix(bases_db, next_db, acc_db)(fixFn)(cmp)(copyInto)
  }

  final def scalaSQLSemiNaiveFOUR[
      Q1,
      Q2,
      Q3,
      Q4,
      T1 >: Tuple,
      T2 >: Tuple,
      T3 >: Tuple,
      T4 >: Tuple,
      P1[_[_]],
      P2[_[_]],
      P3[_[_]],
      P4[_[_]],
      Tables
  ]
    (using Tables =:= (ScalaSQLTable[P1], ScalaSQLTable[P2], ScalaSQLTable[P3], ScalaSQLTable[P4]))
    (set: Boolean)
    (ddb: DuckDBBackend, bases_db: Tables, next_db: Tables, acc_db: Tables)
    (toTuple: (P1[Expr] => Tuple, P2[Expr] => Tuple, P3[Expr] => Tuple, P4[Expr] => Tuple))
    (initBase: () => (query.Select[T1, Q1], query.Select[T2, Q2], query.Select[T3, Q3], query.Select[T4, Q4]))
    (initRecur: Tables => (query.Select[T1, Q1], query.Select[T2, Q2], query.Select[T3, Q3], query.Select[T4, Q4]))
    : Unit = {
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection

    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(bases_db._1)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next_db._1)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc_db._1)}")

    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(bases_db._2)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next_db._2)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc_db._2)}")

    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(bases_db._3)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next_db._3)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc_db._3)}")

    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(bases_db._4)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next_db._4)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc_db._4)}")

    val (base1, base2, base3, base4) = initBase()

    val sqlString1 = "INSERT INTO " + ScalaSQLTable.name(bases_db._1) + " " + db.renderSql(base1)
    val sqlString2 = "INSERT INTO " + ScalaSQLTable.name(bases_db._2) + " " + db.renderSql(base2)
    val sqlString3 = "INSERT INTO " + ScalaSQLTable.name(bases_db._3) + " " + db.renderSql(base3)
    val sqlString4 = "INSERT INTO " + ScalaSQLTable.name(bases_db._4) + " " + db.renderSql(base4)

    ddb.runUpdate(sqlString1)
    ddb.runUpdate(sqlString2)
    ddb.runUpdate(sqlString3)
    ddb.runUpdate(sqlString4)

    def printTable(t: Tables, name: String): Unit =
      println(
        s"${name}1(${ScalaSQLTable.name(t._1)})=${db.runRaw[(Int, String, Int)](s"SELECT * FROM ${ScalaSQLTable.name(t._1)}")}"
      )
      println(
        s"${name}2(${ScalaSQLTable.name(t._2)})=${db.runRaw[(Int, String)](s"SELECT * FROM ${ScalaSQLTable.name(t._2)}")}"
      )
      println(
        s"${name}3(${ScalaSQLTable.name(t._3)})=${db.runRaw[(Int, Int)](s"SELECT * FROM ${ScalaSQLTable.name(t._3)}")}"
      )
      println(
        s"${name}4(${ScalaSQLTable.name(t._4)})=${db.runRaw[(Int, Int)](s"SELECT * FROM ${ScalaSQLTable.name(t._4)}")}"
      )

    val cmp: (Tables, Tables) => Boolean = (next, acc) => {
      val (newDelta1, newDelta2, newDelta3, newDelta4) = (
        next._1.select.asInstanceOf[query.Select[T1, Q1]].except(acc._1.select.asInstanceOf[query.Select[T1, Q1]]),
        next._2.select.asInstanceOf[query.Select[T2, Q2]].except(acc._2.select.asInstanceOf[query.Select[T2, Q2]]),
        next._3.select.asInstanceOf[query.Select[T3, Q3]].except(acc._3.select.asInstanceOf[query.Select[T3, Q3]]),
        next._4.select.asInstanceOf[query.Select[T4, Q4]].except(acc._4.select.asInstanceOf[query.Select[T4, Q4]])
      )

      val str1 = db.renderSql(newDelta1)
      val str2 = db.renderSql(newDelta2)
      val str3 = db.renderSql(newDelta3)
      val str4 = db.renderSql(newDelta4)

      val isEmpty1 = !ddb.runQuery(str1).next()
      val isEmpty2 = !ddb.runQuery(str2).next()
      val isEmpty3 = !ddb.runQuery(str3).next()
      val isEmpty4 = !ddb.runQuery(str4).next()

      isEmpty1 && isEmpty2 && isEmpty3 && isEmpty4
    }

    val fixFn: (Tables, Tables) => Unit = (base, next) => {
      val (query1, query2, query3, query4) = initRecur(base)
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._1)}")
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._2)}")
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._3)}")
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._4)}")

      val sqlString1 = db.renderSql(next._1.insert.select(
        toTuple._1,
        query1
      ))
      ddb.runUpdate(sqlString1)
      val sqlString2 = db.renderSql(next._2.insert.select(
        toTuple._2,
        query2
      ))
      ddb.runUpdate(sqlString2)
      val sqlString3 = db.renderSql(next._3.insert.select(
        toTuple._3,
        query3
      ))
      ddb.runUpdate(sqlString3)
      val sqlString4 = db.renderSql(next._4.insert.select(
        toTuple._4,
        query4
      ))
      ddb.runUpdate(sqlString4)
    }

    val copyInto: (Tables, Tables, Tables) => (Tables, Tables) = (base, acc, next) => {
      val tmp = (
        s"${ScalaSQLTable.name(base._1)}_tmp",
        s"${ScalaSQLTable.name(base._2)}_tmp",
        s"${ScalaSQLTable.name(base._3)}_tmp",
        s"${ScalaSQLTable.name(base._4)}_tmp"
      )
      if (set)
        ddb.runUpdate(s"CREATE TABLE ${tmp._1} AS SELECT * FROM ${ScalaSQLTable.name(base._1)} LIMIT 0")
        ddb.runUpdate(
          s"INSERT INTO ${tmp._1} (SELECT * FROM ${ScalaSQLTable.name(base._1)} UNION SELECT * FROM ${ScalaSQLTable.name(acc._1)})"
        )

        ddb.runUpdate(s"CREATE TABLE ${tmp._2} AS SELECT * FROM ${ScalaSQLTable.name(base._2)} LIMIT 0")
        ddb.runUpdate(
          s"INSERT INTO ${tmp._2} (SELECT * FROM ${ScalaSQLTable.name(base._2)} UNION SELECT * FROM ${ScalaSQLTable.name(acc._2)})"
        )

        ddb.runUpdate(s"CREATE TABLE ${tmp._3} AS SELECT * FROM ${ScalaSQLTable.name(base._3)} LIMIT 0")
        ddb.runUpdate(
          s"INSERT INTO ${tmp._3} (SELECT * FROM ${ScalaSQLTable.name(base._3)} UNION SELECT * FROM ${ScalaSQLTable.name(acc._3)})"
        )

        ddb.runUpdate(s"CREATE TABLE ${tmp._4} AS SELECT * FROM ${ScalaSQLTable.name(base._4)} LIMIT 0")
        ddb.runUpdate(
          s"INSERT INTO ${tmp._4} (SELECT * FROM ${ScalaSQLTable.name(base._4)} UNION SELECT * FROM ${ScalaSQLTable.name(acc._4)})"
        )

        ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc._1)}")
        ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc._2)}")
        ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc._3)}")
        ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc._4)}")

        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._1)} (SELECT * FROM ${tmp._1})")
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._2)} (SELECT * FROM ${tmp._2})")
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._3)} (SELECT * FROM ${tmp._3})")
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._4)} (SELECT * FROM ${tmp._4})")

        ddb.runUpdate(s"DROP TABLE ${tmp._1}")
        ddb.runUpdate(s"DROP TABLE ${tmp._2}")
        ddb.runUpdate(s"DROP TABLE ${tmp._3}")
        ddb.runUpdate(s"DROP TABLE ${tmp._4}")
      else
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._1)} (SELECT * FROM ${ScalaSQLTable.name(base._1)})")
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._2)} (SELECT * FROM ${ScalaSQLTable.name(base._2)})")
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._3)} (SELECT * FROM ${ScalaSQLTable.name(base._3)})")
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._4)} (SELECT * FROM ${ScalaSQLTable.name(base._4)})")

      ddb.runUpdate(s"CREATE TABLE ${tmp._1} AS SELECT * FROM ${ScalaSQLTable.name(base._1)} LIMIT 0")
      ddb.runUpdate(
        s"INSERT INTO ${tmp._1} (SELECT * FROM ${ScalaSQLTable.name(next._1)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc._1)})"
      )
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._1)}")
      ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(next._1)} (SELECT * FROM ${tmp._1})")
      ddb.runUpdate(s"DROP TABLE ${tmp._1}")

      ddb.runUpdate(s"CREATE TABLE ${tmp._2} AS SELECT * FROM ${ScalaSQLTable.name(base._2)} LIMIT 0")
      ddb.runUpdate(
        s"INSERT INTO ${tmp._2} (SELECT * FROM ${ScalaSQLTable.name(next._2)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc._2)})"
      )
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._2)}")
      ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(next._2)} (SELECT * FROM ${tmp._2})")
      ddb.runUpdate(s"DROP TABLE ${tmp._2}")

      ddb.runUpdate(s"CREATE TABLE ${tmp._3} AS SELECT * FROM ${ScalaSQLTable.name(base._3)} LIMIT 0")
      ddb.runUpdate(
        s"INSERT INTO ${tmp._3} (SELECT * FROM ${ScalaSQLTable.name(next._3)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc._3)})"
      )
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._3)}")
      ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(next._3)} (SELECT * FROM ${tmp._3})")
      ddb.runUpdate(s"DROP TABLE ${tmp._3}")

      ddb.runUpdate(s"CREATE TABLE ${tmp._4} AS SELECT * FROM ${ScalaSQLTable.name(base._4)} LIMIT 0")
      ddb.runUpdate(
        s"INSERT INTO ${tmp._4} (SELECT * FROM ${ScalaSQLTable.name(next._4)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc._4)})"
      )
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._4)}")
      ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(next._4)} (SELECT * FROM ${tmp._4})")
      ddb.runUpdate(s"DROP TABLE ${tmp._4}")
      (base, next)
    }

    scalaSQLMultiFix(bases_db, next_db, acc_db)(fixFn)(cmp)(copyInto)
  }

  final def agg_scalaSQLSemiNaive[Q, T >: Tuple, P[_[_]]]
    (set: Boolean)
    (ddb: DuckDBBackend, bases_db: ScalaSQLTable[P], next_db: ScalaSQLTable[P], acc_db: ScalaSQLTable[P])
    (toTuple: P[Expr] => Tuple)
    (initBase: () => String)
    (initRecur: ScalaSQLTable[P] => String)
    : Unit = {
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(bases_db)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next_db)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc_db)}")

    val sqlString = s"INSERT INTO ${ScalaSQLTable.name(bases_db)} (${initBase()})"
    ddb.runUpdate(sqlString)

    val cmp: (ScalaSQLTable[P], ScalaSQLTable[P]) => Boolean = (next, acc) =>
      val str = s"SELECT * FROM ${ScalaSQLTable.name(next)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc)}"
      val isEmpty = !ddb.runQuery(str).next()
      isEmpty

    val fixFn: (ScalaSQLTable[P], ScalaSQLTable[P]) => Unit = (bases, next) => {
      val query = initRecur(bases)
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next)}")

      val sqlString = s"INSERT INTO ${ScalaSQLTable.name(next)} ($query)"
      ddb.runUpdate(sqlString)
    }

    val copyTo: (ScalaSQLTable[P], ScalaSQLTable[P], ScalaSQLTable[P]) => (ScalaSQLTable[P], ScalaSQLTable[P]) =
      (bases, acc, next) => {
        val tmp = s"${ScalaSQLTable.name(bases)}_tmp"
        if (set)
          ddb.runUpdate(s"CREATE TABLE $tmp AS SELECT * FROM ${ScalaSQLTable.name(bases)} LIMIT 0")
          ddb.runUpdate(
            s"INSERT INTO $tmp (SELECT * FROM ${ScalaSQLTable.name(bases)} UNION SELECT * FROM ${ScalaSQLTable.name(acc)})"
          )
          ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc)}")
          ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc)} (SELECT * FROM $tmp)")
          ddb.runUpdate(s"DROP TABLE $tmp")
        else
          ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc)} (SELECT * FROM ${ScalaSQLTable.name(bases)})")

        ddb.runUpdate(s"CREATE TABLE $tmp AS SELECT * FROM ${ScalaSQLTable.name(bases)} LIMIT 0")
        ddb.runUpdate(
          s"INSERT INTO $tmp (SELECT * FROM ${ScalaSQLTable.name(next)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc)})"
        )
        ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next)}")
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(next)} (SELECT * FROM $tmp)")
        ddb.runUpdate(s"DROP TABLE $tmp")

        (bases, next)
      }

    scalaSQLFix(bases_db, next_db, acc_db)(fixFn)(cmp)(copyTo)
  }
  final def agg_scalaSQLSemiNaiveTWO[Q1, Q2, T1 >: Tuple, T2 >: Tuple, P1[_[_]], P2[_[_]], Tables]
    (using Tables =:= (ScalaSQLTable[P1], ScalaSQLTable[P2]))
    (set: Boolean)
    (ddb: DuckDBBackend, bases_db: Tables, next_db: Tables, acc_db: Tables)
    (toTuple: (P1[Expr] => Tuple, P2[Expr] => Tuple))
    (initBase: () => (query.Select[T1, Q1], query.Select[T2, Q2]))
    (initRecur: Tables => (String, String))
    : Unit = {
    val db = ddb.scalaSqlDb.getAutoCommitClientConnection

    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(bases_db._1)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next_db._1)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc_db._1)}")

    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(bases_db._2)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next_db._2)}")
    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc_db._2)}")

    val (base1, base2) = initBase()

    val sqlString1 = "INSERT INTO " + ScalaSQLTable.name(bases_db._1) + " " + db.renderSql(base1)
    val sqlString2 = "INSERT INTO " + ScalaSQLTable.name(bases_db._2) + " " + db.renderSql(base2)

    ddb.runUpdate(sqlString1)
    ddb.runUpdate(sqlString2)

    def printTable(t: Tables, name: String): Unit =
      println(
        s"${name}1(${ScalaSQLTable.name(t._1)})=${db.runRaw[(String)](s"SELECT * FROM ${ScalaSQLTable.name(t._1)}")}"
      )
      println(
        s"${name}2(${ScalaSQLTable.name(t._2)})=${db.runRaw[(String, Int)](s"SELECT * FROM ${ScalaSQLTable.name(t._2)}")}"
      )

    val cmp: (Tables, Tables) => Boolean = (next, acc) => {
      val str1 = s"SELECT * FROM ${ScalaSQLTable.name(next._1)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc._1)}"
      val str2 = s"SELECT * FROM ${ScalaSQLTable.name(next._2)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc._2)}"

      val isEmpty1 = !ddb.runQuery(str1).next()
      val isEmpty2 = !ddb.runQuery(str2).next()
      isEmpty1 && isEmpty2
    }

    val fixFn: (Tables, Tables) => Unit = (base, next) => {
      val (query1, query2) = initRecur(base)
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._1)}")
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._2)}")

      val sqlString1 = s"INSERT INTO ${ScalaSQLTable.name(next._1)} ($query1)"
      ddb.runUpdate(sqlString1)
      val sqlString2 = s"INSERT INTO ${ScalaSQLTable.name(next._2)} ($query2)"
      ddb.runUpdate(sqlString2)
    }
    val copyInto: (Tables, Tables, Tables) => (Tables, Tables) = (base, acc, next) => {
      val tmp = (s"${ScalaSQLTable.name(base._1)}_tmp", s"${ScalaSQLTable.name(base._2)}_tmp")
      if (set)
        ddb.runUpdate(s"CREATE TABLE ${tmp._1} AS SELECT * FROM ${ScalaSQLTable.name(base._1)} LIMIT 0")
        ddb.runUpdate(
          s"INSERT INTO ${tmp._1} (SELECT * FROM ${ScalaSQLTable.name(base._1)} UNION SELECT * FROM ${ScalaSQLTable.name(acc._1)})"
        )

        ddb.runUpdate(s"CREATE TABLE ${tmp._2} AS SELECT * FROM ${ScalaSQLTable.name(base._2)} LIMIT 0")
        ddb.runUpdate(
          s"INSERT INTO ${tmp._2} (SELECT * FROM ${ScalaSQLTable.name(base._2)} UNION SELECT * FROM ${ScalaSQLTable.name(acc._2)})"
        )

        ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc._1)}")
        ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc._2)}")
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._1)} (SELECT * FROM ${tmp._1})")
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._2)} (SELECT * FROM ${tmp._2})")
        ddb.runUpdate(s"DROP TABLE ${tmp._1}")
        ddb.runUpdate(s"DROP TABLE ${tmp._2}")
      else
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._1)} (SELECT * FROM ${ScalaSQLTable.name(base._1)})")
        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._2)} (SELECT * FROM ${ScalaSQLTable.name(base._2)})")

      ddb.runUpdate(s"CREATE TABLE ${tmp._1} AS SELECT * FROM ${ScalaSQLTable.name(base._1)} LIMIT 0")
      ddb.runUpdate(
        s"INSERT INTO ${tmp._1} (SELECT * FROM ${ScalaSQLTable.name(next._1)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc._1)})"
      )
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._1)}")
      ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(next._1)} (SELECT * FROM ${tmp._1})")
      ddb.runUpdate(s"DROP TABLE ${tmp._1}")

      ddb.runUpdate(s"CREATE TABLE ${tmp._2} AS SELECT * FROM ${ScalaSQLTable.name(base._2)} LIMIT 0")
      ddb.runUpdate(
        s"INSERT INTO ${tmp._2} (SELECT * FROM ${ScalaSQLTable.name(next._2)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc._2)})"
      )
      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._2)}")
      ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(next._2)} (SELECT * FROM ${tmp._2})")
      ddb.runUpdate(s"DROP TABLE ${tmp._2}")
      (base, next)
    }

    scalaSQLMultiFix(bases_db, next_db, acc_db)(fixFn)(cmp)(copyInto)
  }
}
