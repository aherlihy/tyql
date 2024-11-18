package tyql

// TODO Quill also allows you to chain a few of them and offers uppercase and lowercase options
// https://github.com/fwbrasil/quill/#naming-strategy

enum CaseConvention:
  case Exact
  case Underscores // three_letter_word
  case PascalCase  // ThreeLetterWord
  case CamelCase   // threeLetterWord

  def convert(name: String): String =
    val parts = CaseConvention.splitName(name)
    this match
      case Exact => name
      case Underscores => parts.mkString("_")
      case PascalCase => parts.map(_.capitalize).mkString
      case CamelCase => parts.head + parts.tail.map(_.capitalize).mkString

object CaseConvention:
  private def splitName(name: String): List[String] =
    name.split("(?=[A-Z])|[_\\s]").filter(_.nonEmpty).map(_.toLowerCase).toList

trait Config (
  val caseConvention: CaseConvention
)

object Config:
  given Config = new Config(CaseConvention.Exact) {}
