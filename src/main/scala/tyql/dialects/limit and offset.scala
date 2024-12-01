package tyql

object LimitAndOffset:
  // TODO currently unused
  trait Separate extends Dialect:
    override def limitAndOffset(limit: Long, offset: Long): String =
      assert(limit >= 0)
      assert(offset >= 0)
      s"LIMIT $limit OFFSET $offset"

  trait MysqlLike extends Dialect:
    override def limitAndOffset(limit: Long, offset: Long): String =
      assert(limit >= 0)
      assert(offset >= 0)
      s"LIMIT $offset,$limit"
