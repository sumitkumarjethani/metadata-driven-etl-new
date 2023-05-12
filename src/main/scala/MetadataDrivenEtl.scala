import etl.{Extractor, Loader, Validator, Transformer}
import metadata.parser.JsonMetadataParser
import reader.JsonReader
import org.apache.log4j.PropertyConfigurator
import org.apache.log4j.Logger

object MetadataDrivenEtl {
  // Cargar la configuración del logger desde el archivo de recursos
  PropertyConfigurator.configure(getClass.getResource("/log4j.properties"))
  private val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      if (args.length == 0) throw new Exception("Ruta de metadatos no proporcionada")

      // Leer la ruta desde los argumentos de la línea de comandos
      val path = args(0)
      val spark = new SparkSessionWrapper("MetadataDrivenEtl").spark

      // Creamos el JsonReader para la lectura de los metadatos
      logger.info("Leyendo fichero de metadatos...")
      val metadata = new JsonReader(path).readJson()

      logger.info("Convirtiendo los metadatos a flujo de datos (DataFlow)...")
      val dataFlow = new JsonMetadataParser().parse(metadata)

      logger.info("Extrayendo datos desde los ficheros de entrada...")
      val extractor = new Extractor(spark)
      val sourcesMap = extractor.load(dataFlow.sources)

      logger.info("Realizando las validaciones sobre los datos extraidos...")
      val validator = new Validator()
      val sourcesMapValidated = validator.validate(dataFlow.transformations, sourcesMap)

      logger.info("Realizando las transformaciones necesarias...")
      val transformer = new Transformer()
      val transformedMap = transformer.transform(dataFlow.transformations, sourcesMapValidated)

      logger.info("Escribiendo los ficheros necesarios...")
      val loader = new Loader()
      loader.load(dataFlow.sinks, transformedMap)

      logger.info("ETL Terminada...")
      spark.stop()
    } catch {
      case e: Exception => logger.error("Ha ocurrido un error: " + e.getMessage()); sys.exit()
    }
  }
}
