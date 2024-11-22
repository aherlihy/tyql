package tyql

trait DialectFeature

object DialectFeature:
  trait RandomFloat extends DialectFeature
  trait RandomUUID extends DialectFeature
  trait RandomIntegerInInclusiveRange extends DialectFeature

  trait ReversibleStrings extends DialectFeature
