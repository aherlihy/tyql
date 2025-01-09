package tyql.bench

import org.openjdk.jmh.annotations.*
import java.util.concurrent.TimeUnit

import tyql._
import scala.language.experimental.namedTuples
import scala.NamedTuple.*
import scala.language.implicitConversions
given tyql.Config = new tyql.Config(tyql.CaseConvention.Underscores, tyql.ParameterStyle.EscapedInline) {}
import tyql.Dialect.mariadb.given
import io.getquill._
import io.getquill.context.jdbc.JdbcContext
import io.getquill.MysqlJdbcContext
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

final case class Person(pid: Long, name: String, age: Int)
final case class Orders(oid: Long, personId: Long, orderDate: String)
final case class Data(a: Option[Long], b: Option[Long], c: Option[Double], d: Option[Double], e: Option[String], f: Option[String])

import tyql.Dialect.mysql.given

/**
CREATE TABLE IF NOT EXISTS person (
                                      pid BIGINT PRIMARY KEY,
                                      name VARCHAR(255) NOT NULL,
                                      age INT NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
                                      oid BIGINT PRIMARY KEY AUTO_INCREMENT,
                                      person_id BIGINT NOT NULL,
                                      order_date VARCHAR(255) NOT NULL,
                                      FOREIGN KEY (person_id) REFERENCES person(pid)
);
INSERT INTO person (pid, name, age) VALUES
                                       (1, 'Zoe Smith', 25),
                                       (2, 'Yara Johnson', 19),
                                       (3, 'Xavier Brown', 30),
                                       (4, 'William Davis', 22),
                                       (5, 'Victoria Wilson', 17),
                                       (6, 'Thomas Anderson', 28),
                                       (7, 'Sarah Miller', 21),
                                       (8, 'Robert Taylor', 16),
                                       (9, 'Quinn Martinez', 24),
                                       (10, 'Peter White', 29),
                                       (11, 'Olivia Garcia', 20),
                                       (12, 'Nathan Lee', 15),
                                       (13, 'Michelle Clark', 26),
                                       (14, 'Luke Thompson', 31),
                                       (15, 'Karen Rodriguez', 18),
                                       (16, 'John Wilson', 27),
                                       (17, 'Isabella Moore', 23),
                                       (18, 'Henry Jackson', 17),
                                       (19, 'Grace Lewis', 32),
                                       (20, 'Frank Martin', 28),
                                       (21, 'Emma Davis', 24),
                                       (22, 'David Chen', 19),
                                       (23, 'Catherine Kim', 33),
                                       (24, 'Bob Williams', 16),
                                       (25, 'Alice Brown', 29);
  */

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 15, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class QueryGenerationBenchmark:
  var conn: java.sql.Connection = null
  var db: tyql.DB = null
  var cachedQuery1 = (Table[Person]()
      .filter(p => p.age > 18)
      .sortDesc(p => p.name)).toQueryIR
  var cachedQuery2 = (for
        p <- Table[Person]()
        if p.age > 18
        o1 <- Table[Orders]()
        o2 <- Table[Orders]().rightJoinOn(o2 => o2.personId > o1.oid)
        if o1.personId == p.pid
      yield (personId = o2.personId)).toQueryIR
  import io.getquill._
  val config = new HikariConfig()
  config.setJdbcUrl("jdbc:mysql://localhost:3307/testdb")
  config.setUsername("testuser")
  config.setPassword("testpass")
  config.setDriverClassName("com.mysql.cj.jdbc.Driver")
  val dataSource = new HikariDataSource(config)
  val ctx = new MysqlJdbcContext(SnakeCase, dataSource)
  import ctx._

  @Setup
  def setup(): Unit =
    Class.forName("com.mysql.cj.jdbc.Driver")
    conn = java.sql.DriverManager.getConnection("jdbc:mysql://localhost:3307/testdb", "testuser", "testpass")
    db = DB(conn)
    cachedQuery1.toSQLQuery()
    cachedQuery2.toSQLQuery()

  @Benchmark
  def read100kRowsTyql(): Unit = {
    db.run(Table[Person]().limit(100000))
  }

  @Benchmark
  def read100kRowsQuill(): Unit = {
    ctx.run(quote {
      query[Person].take(100000)
    })
  }

  // @Benchmark
  // def generateSimpleQuery() : Unit = {
  //   val q = Table[Person]()
  //     .filter(p => p.age > 18)
  //     .sortDesc(p => p.name)
  //   q.toQueryIR.toSQLQuery()
  // }

  // @Benchmark
  // def generateSimpleQueryJustTheIR() : Unit = {
  //   val q = Table[Person]()
  //     .filter(p => p.age > 18)
  //     .sortDesc(p => p.name)
  //   q.toQueryIR
  // }

  // @Benchmark
  // def generateComplexQuery() : Unit = {
  //   val q =
  //     for
  //       p <- Table[Person]()
  //       if p.age > 18
  //       o1 <- Table[Orders]()
  //       o2 <- Table[Orders]().rightJoinOn(o2 => o2.personId > o1.oid)
  //       if o1.personId == p.pid
  //     yield (personId = o2.personId)
  //   q.toQueryIR.toSQLQuery()
  // }

  //   @Benchmark
  // def generateComplexQueryJustTheIR() : Unit = {
  //   val q =
  //     for
  //       p <- Table[Person]()
  //       if p.age > 18
  //       o1 <- Table[Orders]()
  //       o2 <- Table[Orders]().rightJoinOn(o2 => o2.personId > o1.oid)
  //       if o1.personId == p.pid
  //     yield (personId = o2.personId)
  //   q.toQueryIR
  // }

  //   @Benchmark
  // def generateSimpleQueryFromCache() : Unit = {
  //   cachedQuery1.toSQLQuery()
  // }

  // @Benchmark
  // def generateComplexQueryFromCache() : Unit = {
  //   cachedQuery2.toSQLQuery()
  // }

  // @Benchmark
  // def runGenerateSimpleQuery() : Unit = {
  //   val q = Table[Person]()
  //     .filter(p => p.age > 18)
  //     .sortDesc(p => p.name)
  //   db.run(q)
  // }

  // @Benchmark
  // def runGenerateComplexQuery() : Unit = {
  //   val q =
  //     for
  //       p <- Table[Person]()
  //       if p.age > 18
  //       o1 <- Table[Orders]()
  //       o2 <- Table[Orders]().joinOn(o2 => o2.personId > o1.oid)
  //       if o1.personId == p.pid
  //     yield (personId = o2.personId)
  //   db.run(q)
  // }

  // @Benchmark
  // def quillSimpleQuery(): Unit = {
  //   ctx.run(quote {
  //     query[Person].filter(p => p.age > 18).sortBy(p => p.name)(io.getquill.Ord.desc)
  //   })
  // }

  // @Benchmark
  // def quillComplexQuery(): Unit = {
  //   ctx.run(quote {
  //     for {
  //       p <- query[Person] if p.age > 18
  //       o1 <- query[Orders]
  //       o2 <- query[Orders].join(o2 => o2.personId > o1.oid)
  //       if o1.personId == p.pid
  //     } yield o2
  //   })
  // }

  // @Param(Array("100", "1000", "10000"))
  // var size: Int = _
  // @Benchmark
  // def parameterizedBenchmark(): Int =
  //   (1 to size).sum
