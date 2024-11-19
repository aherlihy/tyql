package tyql

trait DialectFeature

object DialectFeature:
  trait RandomFloat(val funName: Option[String], val rawSQL: Option[String] = None) extends DialectFeature:
    assert(funName.isDefined == !rawSQL.isDefined)
