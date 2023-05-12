package writer
import metadata.components.types.SaveModeType.SaveModeType
import org.apache.spark.sql.DataFrame

trait Writer {
  def write(df: DataFrame, path: String, fileName: String, saveMode: SaveModeType): Unit
}
