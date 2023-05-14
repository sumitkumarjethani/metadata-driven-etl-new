package reader

trait Reader {
  def read(path: String): Any
}
