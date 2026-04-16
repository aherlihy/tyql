package test.query.backendspecialization

/** The same TyQL source generates different SQL depending on which dialect
 *  is in scope.  POC; the full dialect catalogue, per-vendor quoting rules,
 *  and feature flags live on the `backend-specialization` branch. */
class BackendSpecializationTest extends munit.FunSuite:

  import tyql.*

  type Row = (id: Int, name: String)
  val t: Table[Row] = Table[Row]("users")

  private def buildInsert(using dialects.Dialect, dialects.DialectFeature.Insertable): String =
    val src: Query[Row, BagResult] = t
    src.insertInto(t).toSQLString

  test("postgresql dialect: identifiers double-quoted"):
    import tyql.dialects.postgresql.given
    val sql = buildInsert
    assert(sql.contains("\"users\""), sql)
    assert(sql.contains("\"id\"") && sql.contains("\"name\""), sql)
    assert(!sql.contains("`"), sql)

  test("mysql dialect: identifiers backtick-quoted"):
    import tyql.dialects.mysql.given
    val sql = buildInsert
    assert(sql.contains("`users`"), sql)
    assert(sql.contains("`id`") && sql.contains("`name`"), sql)
    assert(!sql.contains("\"users\""), sql)

  test("Dialect.name() reflects the imported dialect"):
    locally:
      import tyql.dialects.postgresql.given
      assertEquals(summon[dialects.Dialect].name(), "PostgreSQL Dialect")
    locally:
      import tyql.dialects.mysql.given
      assertEquals(summon[dialects.Dialect].name(), "MySQL Dialect")

  test("generated SQL differs strictly between dialects"):
    val pgSql: String =
      locally:
        import tyql.dialects.postgresql.given
        buildInsert
    val mySql: String =
      locally:
        import tyql.dialects.mysql.given
        buildInsert
    assertNotEquals(pgSql, mySql)
