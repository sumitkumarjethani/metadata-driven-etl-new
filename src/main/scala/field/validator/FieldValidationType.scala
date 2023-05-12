package field.validator

object FieldValidationType extends Enumeration {
  type FieldValidationType = Value
  val NOT_NULL = Value

  def fromString(s: String): FieldValidationType = s match {
    case "NOT_NULL" => NOT_NULL
    case _ => throw new IllegalArgumentException(s"Invalid FieldValidationType: $s")
  }
}
