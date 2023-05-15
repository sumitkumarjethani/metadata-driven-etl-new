package transformer.field.additor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{concat_ws, col}

class FieldAdditorConcat(newFieldName: String, separator: String,
                         inputFieldNames: List[String]) extends FieldAdditor {
  override def add(df: DataFrame): DataFrame = {
    if (!inputFieldNames.forall(df.columns.toList.contains))
      throw new Exception(s"Alguna columna: ${inputFieldNames.toString()} no presente en DF")

    df.withColumn(newFieldName, concat_ws(separator, inputFieldNames.map(col): _*))
  }
}
