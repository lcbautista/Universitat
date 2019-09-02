@SerialVersionUID(101L)
class FileCompression(_map: Map[String,BitVector]) extends Serializable{
  
  def map(): Map[String,BitVector] = {
    _map
  }
  def value(_key: String): BitVector = {
    map()(_key)
  }
}
