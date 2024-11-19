package tyql

trait DialectFeature

object DialectFeature:
  trait RandomFloat(val funName: Option[String], val rawSQL: Option[String] = None) extends DialectFeature:
    assert(funName.isDefined == !rawSQL.isDefined)
  trait RandomUUID(val funName: String) extends DialectFeature
  trait RandomIntegerInInclusiveRange(val expr: (String, String) => String) extends DialectFeature // TODO later change it to not use raw SQL maybe?
