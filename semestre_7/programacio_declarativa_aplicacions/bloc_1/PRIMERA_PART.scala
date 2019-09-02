object Huffman {

abstract class CodeTree

  case class Fork(
      left: CodeTree,              //subarbre
      right: CodeTree,             //subarbre
      chars: List[Char],           //contingut
      weight: Int                  //pes
      ) extends CodeTree
      
  case class Leaf(
      char: Char,                  //caràcter
      weight: Int                  //freqüencia del caràcter
      ) extends CodeTree

  // Part 1

  def weight(tree: CodeTree): Int = tree match {
    case Leaf(c,w) => w;
    case Fork(l,r,c,w) => w;  
  }

  def chars(tree: CodeTree): List[Char] = tree match {
    case Leaf(c,w) => List(c);
    case Fork(l,r,c,w) => c;
  }

  def printCodeTree(tree: CodeTree) = tree match{
  	case Leaf(c,w) => print("(" + c + ")" + w);
    case Fork(l,r,c,w) => 
    	print("(")
    	for(car <- c) print(car);
    	print(")" + w)
  }
  
  def string2Chars(str: String): List[Char] = str.toList
   
  /***************************************************************
  *************Generant CodeTrees*********************************
  ***************************************************************/
     
  def makeCodeTree(left: CodeTree, right: CodeTree) = Fork(left,right,chars(left):::chars(right),weight(left)+weight(right));
  
  /**Donada una llista de caracters, retorna la llista de parells de caracters
  que hi apareixen amb la seva freqüència*/
  def times(chars: List[Char]): List[(Char, Int)] = {//furula
    chars.groupBy(identity).mapValues(_.size).toList
  }
  
  /**Donada una llista de parells caràcters-freqüències, retorna la llista de
  fulles que està ordenada ascendentment segons la freqüència.*/
  def makeOrderedLeafList(freqs: List[(Char, Int)]): List[Leaf] = {//furula
  	//eficiencia: n log n
  	mergeSort(freqs)
  }
  
	def mergeSort(xs: List[(Char, Int)]): List[Leaf] = {//furula
	  val n = xs.length / 2
	  //nomes un element -> retorno el Leaf
	  if (n == 0) 
	  	List(Leaf(xs(0)._1,xs(0)._2))
	  else {
	    def merge(xs: List[Leaf], ys: List[Leaf]): List[Leaf] = (xs, ys) match {
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
  def singleton(trees: List[CodeTree]): Boolean = {//de moment faig aixo per provar until
  	//amb les proves que he fet sembla que es aixi, tot i que no estic gens segur
  	trees.length == 1  		
  }

  /**Donada una llista d'arbres de Huffman, primer en treu els dos arbres amb menys pes
   i els fusiona creant un nou arbre amb un node Fork, tot afegint-lo a la llista
  mantenint l'ordre per pes.*/
  def combine(trees: List[CodeTree]): List[CodeTree] = {//furula
  	//suponemos que trees estara ordenada ascendientemente.
  	var res:List[CodeTree] = List()
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
  def until(a: List[CodeTree] => Boolean, b: List[CodeTree] => List[CodeTree])(c: List[CodeTree]): CodeTree={
  	if(a(c))
  		c(0)
		else
  		until(a,b)(b(c))  		
  }
  
  def createCodeTree(chars: List[Char]): CodeTree = {
  	until(singleton,combine)(makeOrderedLeafList((times(chars))))
  }
  
  /***************************************************************
  *************Descodificant**************************************
  ***************************************************************/
  
  type Bit = Int

  def decode(tree: CodeTree, bits: List[Bit]): List[Char] = {//furula
  	if (bits.length > 0)
  		decodeIn(tree,tree,bits)
  	else
  			List()
  }
  
  def decodeIn(root: CodeTree, currentNode: CodeTree, bits: List[Bit]): List[Char] = currentNode match{//furula
  	case Leaf(c,w) => 
  		if (bits.length > 0)
  			List(c):::decodeIn(root,root,bits)
			else
				//chars(currentNode) -- esto se ha cambiado, por testear
				List(c)
		//no vindria malament controlar la longitud també aquí. Si la llista ja esta buida
		//llavors llençar una excepció o algo.
  	case Fork(l,r,c,w) =>
  		if (bits(0) == 0)
  			decodeIn(root,l,bits.tail)
			else
				decodeIn(root,r,bits.tail)	  	  		
  }

  /**
   * frenchcode és un codi de Huffman pel francès.
   * Generat de 
   *   http://fr.wikipedia.org/wiki/Frèquence_d'apparition_des_lettres_en_français
   */
  val frenchCode: CodeTree = Fork(Fork(Fork(Leaf('s',121895),Fork(Leaf('d',56269),Fork(Fork(Fork(Leaf('x',5928),Leaf('j',8351),List('x','j'),14279),Leaf('f',16351),List('x','j','f'),30630),Fork(Fork(Fork(Fork(Leaf('z',2093),Fork(Leaf('k',745),Leaf('w',1747),List('k','w'),2492),List('z','k','w'),4585),Leaf('y',4725),List('z','k','w','y'),9310),Leaf('h',11298),List('z','k','w','y','h'),20608),Leaf('q',20889),List('z','k','w','y','h','q'),41497),List('x','j','f','z','k','w','y','h','q'),72127),List('d','x','j','f','z','k','w','y','h','q'),128396),List('s','d','x','j','f','z','k','w','y','h','q'),250291),Fork(Fork(Leaf('o',82762),Leaf('l',83668),List('o','l'),166430),Fork(Fork(Leaf('m',45521),Leaf('p',46335),List('m','p'),91856),Leaf('u',96785),List('m','p','u'),188641),List('o','l','m','p','u'),355071),List('s','d','x','j','f','z','k','w','y','h','q','o','l','m','p','u'),605362),Fork(Fork(Fork(Leaf('r',100500),Fork(Leaf('c',50003),Fork(Leaf('v',24975),Fork(Leaf('g',13288),Leaf('b',13822),List('g','b'),27110),List('v','g','b'),52085),List('c','v','g','b'),102088),List('r','c','v','g','b'),202588),Fork(Leaf('n',108812),Leaf('t',111103),List('n','t'),219915),List('r','c','v','g','b','n','t'),422503),Fork(Leaf('e',225947),Fork(Leaf('i',115465),Leaf('a',117110),List('i','a'),232575),List('e','i','a'),458522),List('r','c','v','g','b','n','t','e','i','a'),881025),List('s','d','x','j','f','z','k','w','y','h','q','o','l','m','p','u','r','c','v','g','b','n','t','e','i','a'),1486387)

  /**
   * que ens diu el següent secret en francès?
   */
  val secret: List[Bit] = List(0,0,1,1,1,0,1,0,1,1,1,0,0,1,1,0,1,0,0,1,1,0,1,0,1,1,0,0,1,1,1,1,1,0,1,0,1,1,0,0,0,0,1,0,1,1,1,0,0,1,0,0,1,0,0,0,1,0,0,0,1,0,1)

  /**
   * Escriu una funció que ens ho digui.
   */
  def decodedSecret: List[Char] = {//furula
  	decode(frenchCode,secret)
  }

  /***************************************************************
  *************Codificar******************************************
  ***************************************************************/
  
  def encode(tree: CodeTree)(text: List[Char]): List[Bit] = {//furula
  	encodeIn(tree,tree,text,List())
  }
  
  def encodeIn(root: CodeTree, currentNode: CodeTree, text: List[Char], encoded: List[Bit]): List[Bit] = {//furula
  	if(text.length > 0){
	  	currentNode match{
	  		case Leaf(c,w) => encodeIn(root,root,text.tail,encoded)
	  		case Fork(l,r,c,w) =>
	  			if (chars(l).contains(text(0)))
	  					encodeIn(root,l,text,encoded:::List(0))
					else
	  					encodeIn(root,r,text,encoded:::List(1))
	  	}  		
  	}
  	else
  		encoded
  }

  /***************************************************************
  *************Millorant la eficiència****************************
  ***************************************************************/

  type CodeTable = List[(Char, List[Bit])]
  
  //Retorna la llista de bits del caràcter que volguem
  //d'acord amb la taula que ens passen.
  def codeBits(table: CodeTable)(char: Char): List[Bit] = {
  	var i = 0
  	while (i<table.length && table(i)._1 != char)
  		i += 1
		if (i < table.length)
			table(i)._2
		else
			List()
  }

  /**Crea la taula.*/
  def convert(tree: CodeTree): CodeTable = {//furula
  	tree match{
  		case Leaf(c,w) => List((c,List()))
  		case Fork(l,r,c,w) => mergeCodeTables(convert(l),convert(r))
  	}
  }

  /**Retorna la taula resultant de combinar les taules a i b*/
  def mergeCodeTables(a: CodeTable, b: CodeTable): CodeTable = {//furula
  	var res:CodeTable = List()
  	for(e <- a){
  		res = (e._1,0::e._2)::res
  	}
  	for(e <- b){
  		res = (e._1,1::e._2)::res
  	}
  	res
  }

  /**Retorna una llista de bits resultant de la codificació del text
  utilitzant un arbre de Hoffman*/
  def quickEncode(tree: CodeTree)(text: List[Char]): List[Bit] = {
  	var res:List[Bit] = List()
  	val table = convert(tree)
  	for(c <- text){
  		res = res:::codeBits(table)(c)
  	}
  	res
  }
}
