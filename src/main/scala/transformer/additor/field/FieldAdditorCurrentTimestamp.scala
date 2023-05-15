package transformer.additor.field

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.current_timestamp

class FieldAdditorCurrentTimestamp(fieldName: String) extends FieldAdditor {
  override def add(df: DataFrame): DataFrame = {
    /**
     * Agrega una columna con el nombre del campo especificado y la marca de tiempo actual a un DataFrame.
     *
     * @param df El DataFrame al que se agregará la columna.
     * @return Un nuevo DataFrame con la columna añadida.
     */
    df.withColumn(fieldName, current_timestamp())
  }
}
