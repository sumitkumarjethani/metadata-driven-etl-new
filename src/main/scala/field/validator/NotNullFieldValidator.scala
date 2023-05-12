package field.validator

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class NotNullFieldValidator extends FieldValidator {
  override def validate(fieldName: String, df: DataFrame): DataFrame = {
    // Comprobar si la columna existe en el dataframe
    if (!df.columns.contains(fieldName)) {
      throw new Exception(s"El campo $fieldName no existe.")
    }

    // Comprobar si la columna "arraycoderrorbyfield" existe en el dataframe
    val updatedDf = if (df.columns.contains("arraycoderrorbyfield")) {
      df
    } else {
      df.withColumn("arraycoderrorbyfield", array())
    }

    // Comprobar por fila si la columna es nula y añadir error en caso afirmativo
    val newDf = updatedDf.withColumn("arraycoderrorbyfield",
      when(col(fieldName).isNull, array_union(
        col("arraycoderrorbyfield"),
        array(lit(s"${fieldName}: es nula"))
      )).otherwise(col("arraycoderrorbyfield"))
    )
    newDf
  }
}
