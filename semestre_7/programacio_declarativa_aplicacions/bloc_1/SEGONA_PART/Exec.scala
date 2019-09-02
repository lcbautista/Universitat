    
import java.io.File

import java.nio.file.{Files,Paths}
import scala.actors.Actor
import scala.actors.Actor._

object Exec {
  def main(args: Array[String]):Unit = {

    var args2 = Array[String]()
    /** SI FEM SERVIR INPUT MITJANÇANT PESTANYA ARGUMENTS, COMENTAR AIXÒ **/  
    
    /** COMPRESSIO **/
    
    //var TipusCompressio = "c"
    //var RutaOrigen = "C:\\Users\\aerop\\Desktop\\toCompress\\toCompressMedia"
    //var nActors = "4"
    //var RutaDesti = "C:\\Users\\aerop\\Desktop\\toCompress\\compressed2"
    
    
    /** DESCOMPRESSIO **/
    
    //var TipusCompressio = "d"
    //var RutaOrigen = "C:/Users/aerop/Desktop/filestotest/unziptext/zip"
    //var nActors = "1"
    //var RutaDesti = "C:/Users/aerop/Desktop/filestotest/unziptext/"
   
    
    /** FI PART INPUT MANUAL **/
    
    /** SI VOLEM UTILITZAR ELS ARGUMENTS DEL MAIN, DESCOMENTEM AQUESTA LINIA **/
    args2 = args
    /** SI NO, AQUESTA ALTRA **/
    //args2 = Array(TipusCompressio,RutaOrigen,nActors,RutaDesti)
   
    comprovarArguments(args2)
    
    /** FI PART PESTANYA ARGUMENTS **/
    // ----------------------------------------------------------------------------------------------
    // --------------------------------  INICI PROGRAMA  -------------------------------------------- 
    // ----------------------------------------------------------------------------------------------
    //val fOrigen = new File(args2(0))
  	
    val t1 = System.nanoTime
		
  
    
    var pathOrigen = corregirPath(args2(1))
    var pathDesti = corregirPath(args2(3))
    
    if(args2(0).equals("c")){
    	actor{
    	  val rootOrigen = corregirOrigenComprimir(pathOrigen)
    		val zipper = new ZipperMaster(self,args2(2).toInt,rootOrigen)
    		zipper.start
    		zipper ! Zip(Utils.getAllFiles(pathOrigen))
    		
    		receive{
    			case DoneZip(codeTree,fileCompression) =>{
    				Serializer.serialize(fileCompression, codeTree, args2(3))
    				println("C'est finit")
    				println("Time elapsed: " + (System.nanoTime - t1) / 1e9d)
    				System.exit(0)
    			}
    		}    		
    	}
    }
    else{
      val arxiuSerialitzat = new File(pathOrigen)
      //if (f.exists && f.isDirectory) throw new Exception("Has donat la ruta de una carpeta, s'ha de donar la ruta a l'arxiu a descomprimir")
      

      
      actor {
        val d = Serializer.deserialize(pathOrigen)
      	val huffmanTree = d(0).asInstanceOf[Huffman.CodeTree[Byte]]
      	val fileCompression = d(1).asInstanceOf[FileCompression]
        
        val unzipper = new UnzipperMaster(self, args2(2).toInt, pathDesti)
        unzipper.start
        unzipper ! UnzipAll(huffmanTree, fileCompression.map())  
          
        receive {
          case DoneUnzipAll() => {
            println("C'est finit")
            println("Time elapsed: " + (System.nanoTime - t1) / 1e9d)
            System.exit(0)
          }
        }
      }
    	/*
      try 
      {
    	  val d = Serializer.deserialize(pathOrigen)
    	  val huffmanTree = d(0).asInstanceOf[Huffman.CodeTree[Byte]]
    	  val fileCompression = d(1).asInstanceOf[FileCompression]
    	  
    	  
    	}
      catch {Exception e} {
        throw new Exception("Error al deserialitzar el arxiu")
      }*/
      
      //val d = Serializer.deserialize(args2(1))
    	//val cont = d(0).asInstanceOf[Directory.Contenidor]
    	//val ht = d(1).asInstanceOf[Huffman.CodeTree[Byte]]
    	//val fc = d(2).asInstanceOf[FileCompression]
    	//val prova = fc.map()
    	//println("fin")
    }   
  }
  
  def corregirPath(_path: String): String = {
    val f = new File(_path)
    var res = _path
    if (f.exists && f.isDirectory) {
      res = res.replace("/","\\")
      if (res.charAt(res.length-1) != '\\') res = res + "\\"
    
    }
    res
  }
  def corregirOrigenComprimir(_path: String): String = {
    val f = new File(_path)
    var res = _path
    if (f.exists && f.isDirectory) {
      res = res.replace("/","\\")
      if (res.charAt(res.length-1) != '\\') res = res + "\\"
    
    }
    else if (f.exists && !f.isDirectory) {
      var a = _path
      a = a.replace("\\", "/")
      val parts = a.split("/")
      var i = 0
      var root = ""
      while (i < parts.length-1) {
        root = root + parts(i) + "/"
        i+=1
      }
      root = root.replace("/","\\")
      res = root
      
    }
    res
  }
  
  
  def comprovarArguments(args: Array[String]) = {
    assert(args.size == 4, "Nombre de parametres incorrecte, format: huffzip mode origen nactors desti")
    //Comprovació métode
    assert(args(0).equals("c") || args(0).equals("d"), "Paràmetre mode incorrecte, possibles valors:\n 'c' : Comprimir\n 'd' : Descomprimir")
    //Comprovació ruta origen
    val origen = new File(args(1))
    if (args(0).equals("c")) { 	
      assert(origen.exists, "El arxiu/directori origen no existeix")
    }
    else {
      assert(origen.exists && !origen.isDirectory, "Has de donar la ruta d'origen de un arxiu comprimit")
    }
    //Comprovació num actors
    assert(args(2).toInt > 0, "Parametre amb nombre de actors ha de ser més gran que zero")    
    //Comprovació ruta destí
    val desti = new File(args(3))
    if (args(0).equals("d")) {
      assert(desti.exists && desti.isDirectory, "Has de donar la ruta d'una carpeta existent on descomprimir")  
    }
    
  }
  
  
  
}