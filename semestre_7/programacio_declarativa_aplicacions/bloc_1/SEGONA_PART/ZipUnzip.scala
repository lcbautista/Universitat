import scala.actors.Actor
import scala.collection.mutable.ListBuffer
import java.text.DecimalFormat 

/*************************************************************************************/
/********************MISSATGES QUE S'INTERCANVIARAN***********************************/
/*************************************************************************************/

case class Zip(paths: List[String])
case class DoneZip(codeTree: Huffman.CodeTree[Byte],fc: FileCompression)

case class ZipShards(ct: Huffman.UltimateCodeTable[Byte],shards: ListBuffer[((String,Int),Array[Byte])])
case class DoneZipShards(zippedShards: ListBuffer[((String,Int),BitVector)])

case class Unzip(ct: Huffman.CodeTree[Byte], file: (String,BitVector), path: String)

case class UnzipAll(ct: Huffman.CodeTree[Byte], data: Map[String,BitVector])
case class DoneUnzipAll()
case class DoneUnzip()

case class CalculateFrequency(bytes: Array[Byte])
case class DoneFrequencies(freqs: List[(Byte,Int)])

case class ReduceFreq(freqs: List[(Byte,List[Int])])
case class DoneReduceFreq(reducedFreqs: ListBuffer[(Byte,Int)])

case class ReduceZippedShards(path: String,fileAsZippedShards: Iterable[(Int,BitVector)])
case class DoneReduceZippedShards(zippedFile: (String,BitVector))

case class ReadFile(absolutePath: String)
case class DoneReadFile(absolutePath: String, bytes:Array[Byte])

/**Classe que representa l'Actor esclau encarregat
 * de comprimir i descomprimir fitxers (mapping)*/
class Slave(master: Actor) extends Actor{	
	//COMPORTAMENT COM ACTOR
	def act(){
		while(true){
			receive{
				/**L'esclau calcula i envia al master el nombre de aparacions de cada byte dels bytes que rep.*/
				case CalculateFrequency(bytes)=>{
					val t= System.nanoTime
					master ! DoneFrequencies(Huffman.times(bytes))
					println("Slave: frequency calculated, t.e: " + (System.nanoTime - t) / 1e9d)
				}
				/**L'esclau redueix la llista de aparicions per a cada byte de la llista que rep
				 *  a una llista de (byte,nombre total d'aparicions)*/
				case ReduceFreq(freqs) =>{
					val t = System.nanoTime
					var reducedFreqs = ListBuffer[(Byte,Int)]()
					for(freq <- freqs){
						var total = 0
						for(subfreq <- freq._2){
							total += subfreq
						}
						(freq._1,total) +=: reducedFreqs//prepend: constant time and doesn't creates copies
					}
					master ! DoneReduceFreq(reducedFreqs)
					println("Slave: reduce of frequency done, t.e:" + (System.nanoTime - t) / 1e9d)
				}
				/**L'esclau envia al mestre una llista amb els blocs (shards) resultants
				 * de la compressió utilitzant el CodeTable rebut*/
				case ZipShards(codeTable,shards) =>{
					val t = System.nanoTime
					var zippedBlocs = ListBuffer[((String, Int), BitVector)]()
					for((id,content) <- shards){
						zippedBlocs += ((id,Huffman.quickEncode(content,codeTable)))//constant time and doesn't creates copies
					}
					master ! DoneZipShards(zippedBlocs)
					println("Slave: " + shards.size + " shards zipped, t.e: " + (System.nanoTime - t) / 1e9d)
				}
				/**L'esclau envia al mestre la ruta que rep i un unic bitvector (que correspont al fitxer
				 * representat per la ruta) que és el resultat de reduir els blocs (shards) comprimits*/
				case ReduceZippedShards(path,zippedShards)=>{
					val t = System.nanoTime
					//val sortedBlocs = scala.util.Sorting.stableSort(zippedShards, (e1: (Int, BitVector), e2: (Int, BitVector)) => e1._1 < e2._1)
					var zippedfile = new BitVector(0)
					
					for((nShards,zippedShard) <- zippedShards){
						zippedfile = zippedfile ::: zippedShard
					}
					master ! DoneReduceZippedShards(path,zippedfile.fit)
					println("Slave: reduce of zipped blocs done, t.e: " + (System.nanoTime - t) / 1e9d)
				}
				case Unzip(ct,zippedFile,path)=>{
					val t = System.nanoTime
					val filePath = zippedFile._1	
					println("Decompressing: " + filePath)			
					val unzippedFile = Huffman.decode(ct, zippedFile._2)
					println("Decompressed file: " + filePath + ", t.e: " + (System.nanoTime - t) / 1e9d)
					Utils.writeFile(path,filePath, unzippedFile.toArray)
					master ! DoneUnzip()
				}				
				/**L'esclau envia al mestre el resultat de llegir el fitxer indicat en forma d'array de bytes*/
				case ReadFile(absolutePath: String) => {
					master ! DoneReadFile(absolutePath,Utils.getBytesFromFile(absolutePath))
				}
			}
		}
	}
}

/**Classe que representa l'Actor mestre
 * encarregat de comprimir fitxers (reduce)*/
class ZipperMaster(master: Actor, nSlaves: Int, rootPath: String) extends Actor{
	//ATRIBUTS
	
	/**Frecuencies dels bytes per tal de construir l'arbre de Huffman*/
	private var frequencies = ListBuffer[(Byte,Int)]()
	
	/**Arbre de Huffman que s'utilitzarà per comprimir*/
	private var codeTree:Huffman.CodeTree[Byte] = null
	
	/**Esclaus que faran la feina del mapping*/
	private var slaves = IndexedSeq[Slave]()
	
	/**Llista que guarda els arxius comprimits*/
	private var zippedFiles = List[(String,BitVector)]()
	
	/**Comptador que indica quans workers han acabat la seva feina*/
	private var nWorkersFinished = 0
	
	/***/
	private var intermediates = ListBuffer[(Byte,Int)]()
	
	/**Mapa que guarda els fitxers en forma de bytes. La clau és
	 * la ruta relativa.*/
	private var files = Map[String,Array[Byte]]()
	
	/**Valor que determina la mida dels blocs (shards) resultants de
	 * dividir un fitxer*/
	private var shardSize = 0
	private var intermediatesBlocs = ListBuffer[((String,Int),BitVector)]()
	
	/**Array que guardarà tots els bytes que s'hagin de comprimir.
	 * Només estarà plena les dues primeres passes de la compressió.*/
	private var allBytes = Array[Byte]()
	
	/**Variable que indica quants bytes hi ha per comprimir.*/
	private var bytesToCompress = -1
	
	/**Variable que indica el total de bytes resultant de comprimir
	 * els fitxers*/
	private var bytesCompressed = 0
	
	/**Variable que indica quants fitxers s'han llegit. Només s'utilitza a 
	 * la primera passa de la comressió*/
	private var nReadedFiles = 0
	
	/**Variable que indica el total de fitxers que hi ha per comprimir.*/
	private var nFiles = 0
	
	//METODES AUXILIARS	
	/**Obté la mida del bloc en funció de la mida del fitxer més petit, de la mida de tots els fitxers
	 * i del nombre de workers que hi ha.*/
	private def calculateShardSize(_files: Map[String,Array[Byte]], _nSlaves: Int, _sizeAllFiles: Int): Int={
		var minSize = Int.MaxValue
		//var temp = "none"
		for((path,fileAsBytes) <- _files){
			if(fileAsBytes.size != 0 && fileAsBytes.size < minSize){
				minSize = fileAsBytes.size
				//temp = path
			}
		}
		//println(temp + ": " + minSize)
		while(_nSlaves > _sizeAllFiles/minSize){
			minSize = minSize/2
		}
		minSize
	}
	
	//COMPORTAMENT COM ACTOR
	def act(){
		while(true){
			receive{
				/**El mestre ordena als esclaus llegir els fitxers ubicats a la llista de paths que rep*/
				case Zip(paths)=>{
					nFiles = paths.size
					slaves = Utils.createSlaves(this,nSlaves)
					
					var nSlave = 0
					for(path <- paths){
						if(nSlave == nSlaves)
							nSlave = 0
						slaves(nSlave) ! ReadFile(path)
						nSlave += 1
					}
				}
				/**El mestres ordena als esclaus que calculin la freqüencia d'aparició dels bytes
				 * Els bytes es reparteixen equitativament. Si en total tenim que comprimir 50 bytes
				 * i tenim 5 esclaus, llavors cada esclau calcularà la freqüencia de 10 bytes.*/
				case DoneReadFile(path,bytes) =>{
					nReadedFiles += 1
					allBytes = allBytes ++ bytes
					files += (path -> bytes)
					if(nReadedFiles == nFiles){
						bytesToCompress = allBytes.size
						println("Master: Loading files done")
						var t = System.nanoTime
						shardSize = calculateShardSize(files,nSlaves,allBytes.size)
						println("Shard size: " + shardSize)
	
						var fictionalSizeAllBytes = allBytes.size
						while(fictionalSizeAllBytes % nSlaves != 0){
							fictionalSizeAllBytes += 1
						}					
						val bytesShardSize = fictionalSizeAllBytes/nSlaves
						
						println("Total bytes to compress: " + allBytes.size)
						println("Workers: " + nSlaves) 
						
						//allocate memory
						var bytesShards = Array.ofDim[Byte](nSlaves,bytesShardSize)
		
						var nByteOfShard = 0
						
						var nShard = 0		
						for(byte <- allBytes){
							if(nByteOfShard == bytesShardSize){
								slaves(nShard) ! CalculateFrequency(bytesShards(nShard))
								nByteOfShard = 0
								nShard += 1
							}
							bytesShards(nShard)(nByteOfShard)=byte
							nByteOfShard += 1
						}
						if(nByteOfShard!=0){
							slaves(nShard) ! CalculateFrequency(bytesShards(nShard))
						}
					}					
				}
				/**El mestre crea el dictionari de frequencies per a tal que els esclaus
				 * facin la feina de reducció.
				 * El repartiment de les freqüencies a reduïr es fa equitativament. Si hi ha
				 * 50 bytes amb freqüencies per reduir i tenim 5 esclaus, llavors cada esclau
				 * haura de reduïr les frequencies de 10 bytes.*/
				case DoneFrequencies(freqs)=>{
					nWorkersFinished += 1
					intermediates ++= freqs
											
					if(nWorkersFinished == nSlaves){
						nWorkersFinished = 0
						allBytes = Array.emptyByteArray
									
						var dict = Map[Byte,List[Int]]() withDefault (k => List())	
						for((byte,freq) <- intermediates){
							dict += (byte -> (freq :: dict(byte)))
						}
						intermediates = ListBuffer.empty
						
						val distributedFreqs = Utils.distribute(dict,nSlaves)
						var counter = 0
						for(slave <- slaves){
							slave ! ReduceFreq(distributedFreqs(counter).toList)//constant time and doesnt create copies
							counter += 1
						}						
					}
				}
				/**El mestre un cop té totes les freqüencies de bytes reduïdes, construeix l'arbre de Huffman
				 * i ordena als esclaus comprimir blocs de fitxers (shards).
				 * Cada esclau tindrà un nombre igual de blocs per a comprimir. Per exemple, si hem de comprimir un fitxer
				 * de 50 bytes i tenim 5 actors, llavors es crearan 5 blocs de 10 bytes i per tant cada actor haurà de
				 * comprimir 10 bytes.
				 * La mida dels blocs (quantitat de bytes per bloc) ve determinada per la funció <calculateShardSize>*/
				case DoneReduceFreq(reducedFreqs) =>{
					nWorkersFinished += 1
					frequencies ++= reducedFreqs
					
					if(nWorkersFinished == nSlaves){
						var t = System.nanoTime
						nWorkersFinished = 0
						
						codeTree = Huffman.createCodeTreep(frequencies.toList)//toList is constant time
						val codeTable = Huffman.convert(codeTree)
						val codeTableAsMap = codeTable.toMap
						println("Master: CodeTable created and converted to UltimateCodeTable, t.e: " + (System.nanoTime - t) / 1e9d)
						t = System.nanoTime
						var shards = ListBuffer[((String,Int),Array[Byte])]()
						for(file <- files) {
							shards ++= Utils.toShards(file, shardSize)
						}
						println("Master: All files splitted in shards, t.e: " + (System.nanoTime - t) / 1e9d)
						t = System.nanoTime
						var distributedShards = Utils.distribute(shards,nSlaves)						
						println("Master: Distributed shards, t.e: " + (System.nanoTime - t) / 1e9d)
						
						var nSlave = 0
						for(blocs <- distributedShards){
							slaves(nSlave) ! ZipShards(codeTableAsMap,blocs)
							nSlave += 1
						}
					}
				}
				/**El mestre un cop té tots els blocs comprimits, crea el diccionari de blocs per a tal
				 * que els esclaus facin la feina de reducció.
				 * En aquest cas cada actor haura de reduir un conjunt de blocs del mateix fitxer a nomes
				 * un bloc (bitvector o fitxer comprimit).*/
				case DoneZipShards(zippedShards)=>{
					nWorkersFinished += 1
					intermediatesBlocs ++= zippedShards//not sure if constant time, but at least it doesn't create unnecesary copies
					
					if(nWorkersFinished == nSlaves){
						nWorkersFinished = 0
						
						//with this dict there's no longer necessary to sort the shards because
						//tree set structure keep them ordered
						var dict = Map[String,scala.collection.mutable.TreeSet[(Int,BitVector)]]() withDefault (k => scala.collection.mutable.TreeSet()(tupleOrdering))
												
						//possible improvement: do a ordered insertion of the zipped shard.
						//so when reducing the zipped shards won't be necessary do the sorting.
						//from O(n log n) to (log n)
						//var dictBlocs = Map[String,List[(Int,BitVector)]]() withDefault (k => List())
						
						for(((path,nShard),zippedShard) <- intermediatesBlocs){
							dict += (path -> (dict(path) += ((nShard,zippedShard))))
							//dictBlocs += (path -> ((nShard,zippedShard) :: dictBlocs(path)))
						}
						intermediatesBlocs = ListBuffer.empty
						var sizePerFile = ListBuffer[(String,Int)]()
						for((path,ts) <- dict){
				  		sizePerFile += ((path,ts.last._1+1))
				  	}
						val balancedFilesToReduce = Utils.balance(sizePerFile, nSlaves)
						
						for((path,fileZippedShards) <- dict){
							slaves(balancedFilesToReduce(path)) ! ReduceZippedShards(path,fileZippedShards)				
						}
					}
				}
				/**El mestre un cop ha rebut tots els fitxers comprimits, envia un 
				 * missatge al seu mestre indicat que la compressió ha acabat juntament amb
				 * el CodeTree utilitzat i els fitxers comprimits.*/
				case DoneReduceZippedShards(zippedFile)=>{
					zippedFiles = zippedFile :: zippedFiles
					val bytes = zippedFile._2.size/8
					val compressionRatio = 100*(bytes).toDouble/(files(zippedFile._1).size)
					val df = new DecimalFormat("#.00");
					println("File Compressed [" +df.format(compressionRatio)+"%]: " + zippedFile._1)
					bytesCompressed += bytes
					if(zippedFiles.size == files.size){
						println("Total compression ratio: " + df.format(100*(bytesCompressed).toDouble/bytesToCompress) + "%")
						val mpa = Utils.convertToMap(zippedFiles,rootPath)
						val fc = new FileCompression(mpa)	
						master ! DoneZip(codeTree,fc)
					}
				}
			}
		}
	}
}

class UnzipperMaster(master: Actor, nSlaves: Int, _rootPath: String) extends Actor{
	var slaves = IndexedSeq[Slave]()
	var unzippedFiles = List[(String,Array[Byte])]()
	var nFiles = 0
	var nUnzippedFiles = 0 
	
	def act(){
		while(true){
			receive{
				case UnzipAll(ct,zippedFiles)=>{
				  val t = System.nanoTime
					nFiles = zippedFiles.size
					slaves = Utils.createSlaves(this,nSlaves)
					var sizePerFile = ListBuffer[(String,Int)]()
					for((path,bitVector) <- zippedFiles){
						sizePerFile += ((path,bitVector.size))
					}					
					val balancedFilesToUnzip = Utils.balance(sizePerFile, nSlaves)
					println("Master: Balanced load, t.e: " + (System.nanoTime - t) / 1e9d)
				  for((path,bitVector) <- zippedFiles){
				  	slaves(balancedFilesToUnzip(path)) ! Unzip(ct,(path,bitVector),_rootPath)
				  }					
				}
				case DoneUnzip() =>{
				  nUnzippedFiles += 1
				  if(nUnzippedFiles == nFiles)
				    master ! DoneUnzipAll()
				}
			}
		}
	}
}

/**Comparador de tuples(enter,bitvector) basat en el primer
 * element de la tupla.*/
object tupleOrdering extends Ordering[(Int,BitVector)] {
	def compare(a:(Int,BitVector), b:(Int,BitVector)) = a._1 compare b._1
}