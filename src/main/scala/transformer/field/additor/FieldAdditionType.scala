package transformer.field.additor

object FieldAdditionType extends Enumeration {
  type FieldAdditionType = Value
  val CURRENT_TIMESTAMP = Value

  def fromString(s: String): FieldAdditionType = s match {
    /**
     * Convierte una cadena en un objeto de tipo FieldAdditionType.
     * @param s Cadena a convertir.
     * @return Objeto de tipo FieldAdditionType.
     * @throws IllegalArgumentException si la cadena no corresponde a un tipo válido de FieldAdditionType.
     */
    case "CURRENT_TIMESTAMP" => CURRENT_TIMESTAMP
    case _ => throw new IllegalArgumentException(s"Tipo de addition ${s} no soportado")
  }
}

