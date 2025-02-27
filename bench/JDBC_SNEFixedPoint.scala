package tyql.bench
import scala.collection.mutable
import scala.annotation.experimental

/**
 * Full semi-naive evalution algorithm (supports nonlinear recursion) implemented in non-recursive SQL.
 */
@experimental
object JDBC_SNEFixedPointQuery {

  final def jdbcFix[T](bases: String, next: String, acc: String)
                      (fns: (String, String, String) => Unit)
                      (cmp: (String, String) => Boolean)
                      (copyTo: (String, String, String) => (String, String)): String = {
    if (Thread.currentThread().isInterrupted) throw new Exception("timed out")

    fns(bases, acc, next)

    val isEmpty = cmp(next, acc)
    if (isEmpty)
      acc
      copyTo(bases, acc, next)._1
    else {
      val (newNext, newBase) = copyTo(bases, acc, next)
      jdbcFix(newBase, newNext, acc)(fns)(cmp)(copyTo)
    }
  }

  final def jdbcSemiNaiveIteration(set: Boolean)
                              (ddb: DuckDBBackend, bases_db: String, next_db: String, acc_db: String)
                              (initBase: () => String)
                              (initRecur: (String, String) => String): Unit = {
    ddb.runUpdate(s"DELETE FROM $bases_db")
    ddb.runUpdate(s"DELETE FROM $next_db")
    ddb.runUpdate(s"DELETE FROM $acc_db")

    ddb.runUpdate(s"INSERT INTO $bases_db ${initBase()}")


    val cmp: (String, String) => Boolean = (next, acc) => {
      val newly = s"SELECT * FROM $next EXCEPT SELECT * FROM $acc"
      !ddb.runQuery(newly).next()
    }

    val fixFn: (String, String, String) => Unit = (bases, derived, next) => {
      Helpers.printResultSet(ddb.runQuery(s"SELECT * FROM $bases"), s"DELTA: ${bases}")
      Helpers.printResultSet(ddb.runQuery(s"SELECT * FROM $derived"), s"DERIVED: ${derived}")
      val query = initRecur(bases, derived)
      ddb.runUpdate(s"DELETE FROM $next")
      ddb.runUpdate(s"INSERT INTO $next $query")
      Helpers.printResultSet(ddb.runQuery(s"SELECT * FROM $next"), s"NEXT: ${next}")
    }

    val copyTo: (String, String, String) => (String, String) = (bases, acc, next) => {
      val tmp = s"${bases}_tmp"
      if (set) {
        ddb.runUpdate(s"CREATE TABLE $tmp AS SELECT * FROM $bases LIMIT 0")
        ddb.runUpdate(s"INSERT INTO $tmp (SELECT * FROM $bases UNION SELECT * FROM $acc)")
        ddb.runUpdate(s"DELETE FROM $acc")
        ddb.runUpdate(s"INSERT INTO $acc (SELECT * FROM $tmp)")
        ddb.runUpdate(s"DROP TABLE $tmp")
      } else {
        ddb.runUpdate(s"INSERT INTO $acc (SELECT * FROM $bases)")
      }

      ddb.runUpdate(s"CREATE TABLE $tmp AS SELECT * FROM $bases LIMIT 0")
      ddb.runUpdate(s"INSERT INTO $tmp (SELECT * FROM $next EXCEPT SELECT * FROM $acc)")
      ddb.runUpdate(s"DELETE FROM $next")
      ddb.runUpdate(s"INSERT INTO $next (SELECT * FROM $tmp)")
      ddb.runUpdate(s"DROP TABLE $tmp")

      (bases, next)
    }

    // 1st iteration is naive, not semi-naive
    fixFn(bases_db, bases_db, acc_db)

    jdbcFix(bases_db, next_db, acc_db)(fixFn)(cmp)(copyTo)
  }

//
//  // Mutually recursive relations
//  @annotation.tailrec
//  final def scalaSQLMultiFix[T]
//  (bases: T, next: T, acc: T)
//  (fns: (T, T) => Unit)
//  (cmp: (T, T) => Boolean)
//  (copyTo: (T, T, T) => (T, T))
//  : T = {
//    //    println(s"----- start multifix ------")
//    //    println(s"BASE=${ScalaSQLTable.name(bases.asInstanceOf[Tuple2[ScalaSQLTable[?], ScalaSQLTable[?]]]._1)}")
//    //    println(s"NEXT=${ScalaSQLTable.name(next.asInstanceOf[Tuple2[ScalaSQLTable[?], ScalaSQLTable[?]]]._1)}")
//    //    println(s"ACC=${ScalaSQLTable.name(acc.asInstanceOf[Tuple2[ScalaSQLTable[?], ScalaSQLTable[?]]]._1)}")
//    if (Thread.currentThread().isInterrupted) throw new Exception(s"timed out")
//    fns(bases, next)
//
//    val isEmpty = cmp(next, acc)
//    if (isEmpty)
//      copyTo(bases, acc, next)
//      acc
//    else
//      val (newNext, newBase) = copyTo(bases, acc, next)
//      scalaSQLMultiFix(newBase, newNext, acc)(fns)(cmp)(copyTo)
//  }
//
//  // this is silly but higher kinded types are mega painful to abstract
//  final def scalaSQLSemiNaiveTWO[Q1, Q2, T1 >: Tuple, T2 >: Tuple, P1[_[_]], P2[_[_]], Tables]
//  (using Tables =:= (ScalaSQLTable[P1], ScalaSQLTable[P2]))
//  (set: Boolean)
//  (ddb: DuckDBBackend, bases_db: Tables, next_db: Tables, acc_db: Tables)
//  (toTuple: (P1[Expr] => Tuple, P2[Expr] => Tuple))
//  (initBase: () => (query.Select[T1, Q1], query.Select[T2, Q2]))
//  (initRecur: Tables => (query.Select[T1, Q1], query.Select[T2, Q2]))
//  : Unit = {
//    val db = ddb.scalaSqlDb.getAutoCommitClientConnection
//
//    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(bases_db._1)}")
//    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next_db._1)}")
//    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc_db._1)}")
//
//    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(bases_db._2)}")
//    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next_db._2)}")
//    ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc_db._2)}")
//
//    val (base1, base2) = initBase()
//
//    val sqlString1 = "INSERT INTO " + ScalaSQLTable.name(bases_db._1) + " " + db.renderSql(base1)
//    val sqlString2 = "INSERT INTO " + ScalaSQLTable.name(bases_db._2) + " " + db.renderSql(base2)
//
//    ddb.runUpdate(sqlString1)
//    ddb.runUpdate(sqlString2)
//
//    def printTable(t: Tables, name: String): Unit =
//      println(s"${name}1(${ScalaSQLTable.name(t._1)})=${db.runRaw[(String)](s"SELECT * FROM ${ScalaSQLTable.name(t._1)}")}")
//      println(s"${name}2(${ScalaSQLTable.name(t._2)})=${db.runRaw[(String, Int)](s"SELECT * FROM ${ScalaSQLTable.name(t._2)}")}")
//
//
//    val cmp: (Tables, Tables) => Boolean = (next, acc) => {
//      val str1 = s"SELECT * FROM ${ScalaSQLTable.name(next._1)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc._1)}"
//      val str2 = s"SELECT * FROM ${ScalaSQLTable.name(next._2)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc._2)}"
//
//      val isEmpty1 = !ddb.runQuery(str1).next()
//      val isEmpty2 = !ddb.runQuery(str2).next()
//      isEmpty1 && isEmpty2
//    }
//
//    val fixFn: (Tables, Tables) => Unit = (base, next) => {
//      val (query1, query2) = initRecur(base)
//      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._1)}")
//      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._2)}")
//      val sqlString1 = db.renderSql(next._1.insert.select(
//        toTuple._1,
//        query1
//      ))
//      val sqlString2 = db.renderSql(next._2.insert.select(
//        toTuple._2,
//        query2
//      ))
//      ddb.runUpdate(sqlString1)
//      ddb.runUpdate(sqlString2)
//    }
//
//    val copyInto: (Tables, Tables, Tables) => (Tables, Tables) = (base, acc, next) => {
//      val tmp = (s"${ScalaSQLTable.name(base._1)}_tmp", s"${ScalaSQLTable.name(base._2)}_tmp")
//      if (set)
//        ddb.runUpdate(s"CREATE TABLE ${tmp._1} AS SELECT * FROM ${ScalaSQLTable.name(base._1)} LIMIT 0")
//        ddb.runUpdate(s"INSERT INTO ${tmp._1} (SELECT * FROM ${ScalaSQLTable.name(base._1)} UNION SELECT * FROM ${ScalaSQLTable.name(acc._1)})")
//
//        ddb.runUpdate(s"CREATE TABLE ${tmp._2} AS SELECT * FROM ${ScalaSQLTable.name(base._2)} LIMIT 0")
//        ddb.runUpdate(s"INSERT INTO ${tmp._2} (SELECT * FROM ${ScalaSQLTable.name(base._2)} UNION SELECT * FROM ${ScalaSQLTable.name(acc._2)})")
//
//        ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc._1)}")
//        ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(acc._2)}")
//        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._1)} (SELECT * FROM ${tmp._1})")
//        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._2)} (SELECT * FROM ${tmp._2})")
//        ddb.runUpdate(s"DROP TABLE ${tmp._1}")
//        ddb.runUpdate(s"DROP TABLE ${tmp._2}")
//
//      else
//        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._1)} (SELECT * FROM ${ScalaSQLTable.name(base._1)})")
//        ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(acc._2)} (SELECT * FROM ${ScalaSQLTable.name(base._2)})")
//
//      ddb.runUpdate(s"CREATE TABLE ${tmp._1} AS SELECT * FROM ${ScalaSQLTable.name(base._1)} LIMIT 0")
//      ddb.runUpdate(s"INSERT INTO ${tmp._1} (SELECT * FROM ${ScalaSQLTable.name(next._1)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc._1)})")
//      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._1)}")
//      ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(next._1)} (SELECT * FROM ${tmp._1})")
//      ddb.runUpdate(s"DROP TABLE ${tmp._1}")
//
//      ddb.runUpdate(s"CREATE TABLE ${tmp._2} AS SELECT * FROM ${ScalaSQLTable.name(base._2)} LIMIT 0")
//      ddb.runUpdate(s"INSERT INTO ${tmp._2} (SELECT * FROM ${ScalaSQLTable.name(next._2)} EXCEPT SELECT * FROM ${ScalaSQLTable.name(acc._2)})")
//      ddb.runUpdate(s"DELETE FROM ${ScalaSQLTable.name(next._2)}")
//      ddb.runUpdate(s"INSERT INTO ${ScalaSQLTable.name(next._2)} (SELECT * FROM ${tmp._2})")
//      ddb.runUpdate(s"DROP TABLE ${tmp._2}")
//
//      (base, next)
//    }
//
//    scalaSQLMultiFix(bases_db, next_db, acc_db)(fixFn)(cmp)(copyInto)
//  }

}