import scala.actors.Actor
import scala.actors.Actor._
import java.io._
import java.nio.file.{Files,Paths}
import scala.reflect._
import scala.collection.mutable.ListBuffer

/**Classe estàtica que conté diversos mètodes d'utilitat*/
object Utils{	
	
	/**Obté el nombre indicat de actors que tenen com a mestre l'actor
	 * indicat*/
	def createSlaves(master: Actor,nSlaves: Int): IndexedSeq[Slave]={
		val slaves = for (i <- 0 until nSlaves) yield new Slave(master)						
		for (slave <- slaves) slave.start()
		slaves
	}
	
	def convertToMap(_list: List[(String,BitVector)], _path: String): Map[String,BitVector] = {
    var map = Map[String,BitVector]() 
    var fileName = ""
    for (file <- _list) {
      fileName = file._1.replace("/","\\")
      val str = fileName.replace(_path,"")
      map += str -> file._2
      
    }
    map
  }
	
	/**Donat la ruta d'un fitxer, retorna els bytes que constitueixen
	 * aquest fitxer.*/
	def getBytesFromFile(_filePath: String): Array[Byte] = {
    val f = new File(_filePath)
    if (f.exists && !f.isDirectory) Files.readAllBytes(f.toPath)
    else if (f.exists && f.isDirectory) throw new Exception("Has de donar la ruta de un fitxer")
    else throw new Exception("El fitxer no existeix")
  }
	
	/**Retorna tots els arxius continguts al root.*/
  def getAllFiles(_path: String): List[String] = {
    val f = new File(_path)
    if (f.isDirectory) {
      var list = List[String]()
      for (file <- f.listFiles) list = list ++ getAllFiles(file.getPath)
      list
    }
    else List[String](f.getPath)
  }
  
  /**Divideix la col·lecció iterable en nParticions.*/
  def distribute[T](collection: Iterable[T], nPartitions: Int): Array[ListBuffer[T]]={  	
  	//determine the number of elements per partition 
  	var nItemsPerActor = collection.size/nPartitions
  	if(collection.size%nPartitions != 0){
  		nItemsPerActor += 1
  	}
  	//Allocate memory
  	var ret = new Array[ListBuffer[T]](nPartitions)
  	
  	for(i<-0 to ret.size-1){
  		ret(i) = new ListBuffer[T]
  	}
  	//fill the partitions
  	var counter = 0
		for(elem <- collection){
			ret(counter) += elem
			counter += 1
			if(counter == nPartitions)
				counter = 0
		}
  	ret
  }		
	
  /**Divideix la representació del fitxer exigida en diversos blocs (shards)
   * segons la mida exigida.*/
	def toShards(file: (String,Array[Byte]), sizeOfBlock: Int): ListBuffer[((String,Int),Array[Byte])] = {
		var ret = ListBuffer[((String,Int),Array[Byte])]()
		val bytes = file._2
		
		//allocate memory
		var nBlocs = bytes.size/sizeOfBlock
		for(i<-0 to nBlocs-1){
			ret += (((file._1,i),new Array[Byte](sizeOfBlock)))
		}
		if(bytes.size % sizeOfBlock != 0){
			nBlocs += 1
			ret += (((file._1,nBlocs-1),new Array[Byte](bytes.size%sizeOfBlock)))
		}
		//fill the shards
		var counterArr = 0
		var counter = 0		
		for(bloc <- ret){
			while(counterArr < bloc._2.size && counter < bytes.size){
				bloc._2(counterArr) = bytes(counter)
				counter += 1
				counterArr += 1
			}
			counterArr = 0
		}
		ret
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
  
  def writeFile(_root: String, _path: String, _info: Array[Byte]) = {
    val root = new File(_root)
    if (!root.exists) root.mkdir
    
    var pathChanged = _path.replace(_root,"")
    pathChanged = pathChanged.replace("\\","/")
    val parts = pathChanged.split("/")
    var i = 0
    var directori = _root
    directori = directori.replace("\\","/")
    var f = new File(directori)
    if (!f.exists) {
      println("Creada carpeta " + f.getName)
      f.mkdir
    }
    while( i < parts.length-1 ){
       var f = new File(directori + "\\" + parts(i))
       if (!f.exists) {
         println("Creada carpeta " + f.getName)
         f.mkdir
       }
       
       directori = directori + "\\" + parts(i)
       i = i + 1;
    }
    val bos = new BufferedOutputStream(new FileOutputStream(_root + _path))
    bos.write(_info)
    bos.close
  }
  
  /**Donat els fitxers comprimits en forma de blocs i un nombre determinat d'actors, es retorna els fitxers
   * que ha de descomprimir cada actor (0...nActor-1) tal que el total de informació que processin
   * estigui balancejat.*/
  def balance(sizePerFile: ListBuffer[(String,Int)], nActors: Int):Map[String,Int]={
  	
  	var ret = Map[String,Int]() 
  	var weights = new Array[Int](nActors)
  	var i = 0
  	while(sizePerFile.size > 0){
	  	var max = 0
	  	var maxS = "none"
	  	var index = 0
	  	var posMax = 0
	  	for((path,size) <- sizePerFile){
	  		if(size > max){
	  			maxS = path
	  			max = size
	  			posMax = index
	  		}
	  		index += 1
	  	}
  		sizePerFile.remove(posMax)
  		var min = Int.MaxValue
  		var posMin = 0
  		for(i <- 0 to weights.size-1){
  			if(weights(i) < min){  				
  				posMin = i
  				min = weights(i)
  			}
  		}
  		weights(posMin) += max
  		ret += (maxS -> posMin)
  		i += 1
  	}
  	ret
  }
  
}
