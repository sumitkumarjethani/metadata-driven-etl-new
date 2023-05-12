Para la ejecucción correcta de metadata driven ETL se deben seguir los siguientes pasos:

1. Modificar el metadata.txt con las rutas del equipo local y con las vailadiciones y transformaciones permitidas.
Si se intenta ejecutar con algún tipo de transformación o parámetro no permitido el programa abortará.

2. Ejecutar el proceso con commando (similiar): ***scala ./MetadataDrivenEtl metadata_path***.
Si no se suministra la ruta del fichero de metadatos el proceso abortará.

Ejemplo del fichero de metadatos (en JSON): ver **metadata.txt** subido


