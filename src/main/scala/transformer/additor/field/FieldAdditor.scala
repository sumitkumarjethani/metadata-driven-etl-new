package transformer.additor.field

import org.apache.spark.sql.DataFrame

trait FieldAdditor {
  def add(df: DataFrame): DataFrame
}
