import java.io._

object Serializer {
  
  class ObjectInputStreamWithCustomClassLoader(_fis: FileInputStream) 
        extends ObjectInputStream(_fis) {
    
    override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
      try {
        Class.forName(desc.getName, false, getClass.getClassLoader) 
      }
      catch {
        case ex: ClassNotFoundException => super.resolveClass(desc)
      }
    }
    
  }
  
  def serialize(_fileCompression: FileCompression, _huffman: Huffman.CodeTree[Byte], _filePath:String) {
    
    val f = new File(_filePath)
    if (!f.exists) f.createNewFile()
    
    val fos = new FileOutputStream(_filePath)
    val oos = new ObjectOutputStream(fos)
    
    oos.writeObject(_huffman)
    oos.writeObject(_fileCompression)
    oos.close
  }
  
  def deserialize(_filePath: String): Array[Any] = {
    val fis = new FileInputStream(_filePath)
    val ois = new ObjectInputStreamWithCustomClassLoader(fis)
    
    val huffman = ois.readObject()
    val fileCompression = ois.readObject()
    ois.close
    Array[Any](huffman,fileCompression)
    
  }  
}