object Huffman {
	import scala.collection.mutable.ListBuffer
	
	@SerialVersionUID(102L)
	abstract class CodeTree[+T] extends Serializable

  case class Fork[T](
      left: CodeTree[T],              //subarbre
      right: CodeTree[T],             //subarbre
      lt: List[T],           //contingut
      weight: Int                  //pes
      ) extends CodeTree
      
  case class Leaf[T](
      t: T,                  //caràcter
      weight: Int                  //freqüencia del caràcter
      ) extends CodeTree

  // Part 1

  def weight[T](tree: CodeTree[T]): Int = tree match {
    case Leaf(t,w) => w;
    case Fork(l,r,lt,w) => w;  
  }

  def content[T](tree: CodeTree[T]): List[T] = tree match {
  	case Leaf(t,w)  => List(t.asInstanceOf[T])
    case Fork(l,r,lt,w) => lt.asInstanceOf[List[T]] 
  }

  /**def printCodeTree[T](tree: CodeTree[T])= tree match{
  	case Leaf(t,w) => print("(" + t.asInstanceOf[T] + ")" + w);
    case Fork(l,r,lt,w) => 
    	print("(")
    	for(t <- lt) print(t.asInstanceOf[T]);
    	print(")" + w)
  }*/
  
  def string2Chars(str: String): List[Char] = str.toList
   
  /***************************************************************
  *************Generant CodeTrees*********************************
  ***************************************************************/
     
  def makeCodeTree[T](left: CodeTree[T], right: CodeTree[T]) = {
  	Fork(left,right,content[T](left):::content[T](right),weight(left)+weight(right));
  }
  
  /**Donada una llista de caracters, retorna la llista de parells de caracters
  que hi apareixen amb la seva freqüència*/
  def times[T](elements: List[T]): List[(T, Int)] = {//furula
    elements.groupBy(identity).mapValues(_.size).toList
  }
  
  def times[T](elements: Array[T]): List[(T,Int)]={
  	elements.groupBy(identity).mapValues(_.size).toList
  }
  
  /**Donada una llista de parells caràcters-freqüències, retorna la llista de
  fulles que està ordenada ascendentment segons la freqüència.*/
  def makeOrderedLeafList[T](freqs: List[(T, Int)]): List[Leaf[T]] = {//furula
  	//eficiencia: n log n
  	mergeSort(freqs)
  }
  
	def mergeSort[T](xs: List[(T, Int)]): List[Leaf[T]] = {//furula
	  val n = xs.length / 2
	  //nomes un element -> retorno el Leaf
	  if (n == 0){
	  	if(xs == Nil){
	  		List()
	  	}
	  	else{
	  		List(Leaf(xs(0)._1,xs(0)._2))
	  	}
	  }	  	
	  else {
	    def merge(xs: List[Leaf[T]], ys: List[Leaf[T]]): List[Leaf[T]] = (xs, ys) match {
	    		//Nil es una llista buida.
	        case(Nil, ys) => ys
	        case(xs, Nil) => xs
	        case(x :: xs1, y :: ys1) =>
	          if (x.weight < y.weight)
	          	x::merge(xs1, ys)
	          else 
	          	y :: merge(xs, ys1)
	    }
	    val (left, right) = xs splitAt(n)
	    merge(mergeSort(left), mergeSort(right))
	  }
	}
  
	/**Donada una llista d'arbres de Huffman, indica si aquesta conté només un arbre.*/
  def singleton[T](trees: List[CodeTree[T]]): Boolean = {
  	trees.length == 1  		
  }

  /**Donada una llista d'arbres de Huffman, primer en treu els dos arbres amb menys pes
   i els fusiona creant un nou arbre amb un node Fork, tot afegint-lo a la llista
  mantenint l'ordre per pes.*/
  def combine[T](trees: List[CodeTree[T]]): List[CodeTree[T]] = {//furula
  	//suponemos que trees estara ordenada ascendientemente.
  	var res:List[CodeTree[T]] = List()
  	val n = trees.length  
  	if (n > 1){
  		val newTree = makeCodeTree(trees(0),trees(1))
  		
  		var inserit = false
  		for(i <- n-1 to 2 by -1){
  			if (newTree.weight < Huffman.weight(trees(i))){
  				res = trees(i)::res
  			}
  			else{
  				if(!inserit)
  					res = newTree::res
  				res = trees(i)::res
  				inserit = true
  			}  			
  		}
  		if(!inserit)
  				res = newTree::res
  	}
  	else
  		res = trees
		res
  }

  /**Crida  les funcions "singleton" i "combine" i s'aplicarà a una llista de CodeTree
  Es cridara fins que a la llista nomes quedi un arbre (l'òptim).*/
  def until[T](a: List[CodeTree[T]] => Boolean, b: List[CodeTree[T]] => List[CodeTree[T]])(c: List[CodeTree[T]]): CodeTree[T]={
  	if(a(c))
  		c(0)
		else
  		until(a,b)(b(c))  		
  }
  
  def createCodeTree[T](elements: List[T]): CodeTree[T] = {
  	until(singleton,combine[T])(makeOrderedLeafList((times(elements))))
  }  
  
  def createCodeTreep[T](freqs: List[(T,Int)]): CodeTree[T] = {
  	until(singleton,combine[T])(makeOrderedLeafList(freqs))
  }
  
  /***************************************************************
  *************Descodificant**************************************
  ***************************************************************/
    
  def decode[T](tree: CodeTree[T], bits: BitVector): List[T] = {//furula
  	var ret = ListBuffer[T]()
  	var bitIndex = 0
  	while(bitIndex < bits.size){
  		val res = decodeIn(tree,bits,bitIndex)
  		ret += res._1//constant time
  		bitIndex = res._2
  	}
  	ret.toList//constant time
  }
  
  private def decodeIn[T]( currentNode: CodeTree[T], bits: BitVector, bitIndex: Int): (T,Int) = currentNode match{//furula
  	case Leaf(t: T,w) =>	(t,bitIndex)
  	case Fork(l: CodeTree[T],r: CodeTree[T],lt,w) =>{
  		if(bitIndex < bits.size){
	  		if(bits.get(bitIndex)){//si es 1
	  			decodeIn(r,bits,bitIndex+1)
	  		}
	  		else{
	  			decodeIn(l,bits,bitIndex+1)
	  		}
  		}
  		else{
  			throw new Exception("Bad coded file or incorrect code tree.")
  			
  		}
  	}
  }
  
  /***************************************************************
  *************Codificar******************************************
  ***************************************************************/
  
  def encode[T](tree: CodeTree[T])(toCode: List[T]): BitVector = {//furula
  	encodeIn(tree,tree,toCode,new BitVector(0))
  }
  
  def encodeIn[T](root: CodeTree[T], currentNode: CodeTree[T], toCode: List[T], encoded: BitVector): BitVector = {//furula
  	if(toCode.length > 0){
	  	currentNode match{
	  		case Leaf(t,w) => encodeIn(root,root,toCode.tail,encoded)
	  		case Fork(l,r,lt,w) =>
	  			if (content(l).contains(toCode(0)))
  					encodeIn(root,l,toCode,encoded.add(false))
					else
  					encodeIn(root,r,toCode,encoded.add(true))
	  	}  		
  	}
  	else
  		encoded
  }

  /***************************************************************
  *************Millorant la eficiència****************************
  ***************************************************************/

  
  type CodeTable[T] = List[(T, BitVector)]
  type UltimateCodeTable[T] = Map[T,BitVector]
  
  /**Retorna la llista de bits del caràcter que volguem
  d'acord amb la taula que ens passen.*/
  def codeBits[T](table: CodeTable[T])(element: T): BitVector = {//furula
  	var i = 0
  	while (i<table.length && table(i)._1 != element)
  		i += 1
		if (i < table.length)
			table(i)._2
		else
			new BitVector(0)
  	//short version:
  	//table.find(_._1 == element).get._2
  }

  /**Crea la taula.*/
  def convert[T](tree: CodeTree[T]): CodeTable[T] = {//furula
  	tree match{
  		case Leaf(t: T,w) => List((t,new BitVector(0)))
  		case Fork(l: CodeTree[T],r: CodeTree[T],lt,w) => mergeCodeTables(convert(l),convert(r))
  	}
  }

  /**Retorna la taula resultant de combinar les taules a i b*/
  def mergeCodeTables[T](a: CodeTable[T], b: CodeTable[T]): CodeTable[T] = {//furula
  	var res:CodeTable[T] = List()
  	for(e <- a){
  		res = (e._1,false::e._2)::res
  	}
  	for(e <- b){
  		res = (e._1,true::e._2)::res
  	}
  	res
  }

  /**Retorna una llista de bits resultant de la codificació del text
  utilitzant un arbre de Hoffman*/
  def quickEncode[T](tree: CodeTree[T])(toCode: List[T]): BitVector = {//furula
  	var res = new BitVector(0)
  	val table = convert(tree)
  	for(c <- toCode){
  		res = res:::codeBits(table)(c)
  	}
  	res
  }
  
  //Auxiliars
  def quickEncode[T](_array: Array[T], _codeTable: UltimateCodeTable[T]): BitVector = {
    var res = new BitVector(0)
    
    for (elem <- _array) {
      res = res ::: _codeTable(elem)
    }
    res
  }
}
