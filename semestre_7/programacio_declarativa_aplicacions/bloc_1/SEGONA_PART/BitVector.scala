import java.io.ObjectOutputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ByteArrayInputStream

/** La classe BitVector ens permet representar un vector de Bits de forma compacta,
 * es a dir, practicament un bit ocupa un bit de memoria: cal afegir-hi l'overhead
 * de l'enter (size) que controla la mida i els bits "sobrants", com a maxim 7, ja que
 * s'agrupen els bits en un Array de bytes (bits).*/
class BitVector(n: Int) extends Serializable {
  /** Fem un Array de bytes amb la mida adequada i
   * inicialitzat a 0.
   * Fixeu-vos que >>3 equival a dividir per 8.
   */
  var size = n;
  var bits = Array.fill((size >> 3) + 1)(0.toByte);

  private def realSize():Int={
  	bits.length*8
  }
  
  /**Actualitza la mida del BitVector en funciÃ³ d'una nova
   * posiciÃ³ donada. Si aquesta posiciÃ³ Ã©s mÃ©s gran que la mida
   * real llavors copia el BitVector a un el doble de gran i s'incrementa
   * en 1 la mida ficticia.
   * Alternativament, si aquesta posiciÃ³ Ã©s mÃ©s gran que la mida
   * ficticia, llavors la mida ficticia s'incrementa en 1.*/
  private def resize(n: Int)={
	  if(n>=realSize){
		  //"real resize")
		  size=n+1
		  var newBitArray=Array.fill(bits.length*2)(0.toByte)
		  for(i <- 0 to bits.length-1){
		  	newBitArray(i)=bits(i) 
		  }
		  bits=newBitArray
	  }
	  else if(n>=size){
	  	//"fake resize")
	  	size=n+1
	  }
  }

  /** Posa el valor del bit n a u. */
  private def set(n : Int) = {
  	resize(n)
  	bits(n >> 3) = (bits(n >> 3) | 1 << (n & 7)).toByte
  }
  
  /** Posa el valor del bit n a zero. */
  private def clear(n : Int) = {
    resize(n)
  	bits(n >> 3) = (bits(n >> 3) & ~(1 << (n & 7))).toByte
  }

  /** Retorna cert si el bit n es u i fals altrament */
  def get(n : Int) : Boolean = {
	  if (n < size)
	    (bits(n >> 3) & (1 << (n & 7))) != 0;
	  else
	  	throw new IllegalArgumentException("Out of bounds");
  }

  /**Retorna el la cua del BitVector corresponent a 'this'*/
  def tail():BitVector={
	  var tail=new BitVector(size-1)
	  for(i <- 1 to size-1){
		  if(get(i)){
		  	tail.set(i-1)
		  }
	  }
	  tail
  }
  
  /** Torna el nombre de bits al vector. Fixem-nos que
   * es una unitat mes gran que la posicio de l'ultim bit valid. */
  def getSize() : Int = {
    size;
  }
  
  /**Afegeix al final del BitVector 'this' el bit 
   * equivalent a la expressiÃ³ booleana: (true,1)(false,0) */
  def add(bit: Boolean):BitVector={
	  if(bit){
	  	set(size);
	  }
	  else{
	  	clear(size);
	  }
	  this
  }
    
  /**Retorna un nou BitVector amb el cap indicat i 
   * la resta amb aquest BitVector*/  
  def ::(bit: Boolean):BitVector={
	  var newBitVector=new BitVector(size+1)
	  if(bit){
	  	newBitVector.set(0)
	  }
	  for(i <- 0 to size-1){
	  	if(get(i)){
	  		newBitVector.set(i+1) 
	  	}
	  }
	  newBitVector
  }
  
  /**Retorna un BitVector resultant de concatenar 
   * 'prefix' amb 'this'.*/
  def :::(prefix: BitVector) : BitVector={
	  for(i <- 0 to size-1){
			prefix.add(get(i))
	  }
	  prefix
  }
  
  /**Retorn un BitVector més petit en la majoria d'ocasions
   * eliminant l'espai reservat ocasionat pel resize automàtic
   * que està sense utilitzar*/
  def fit() : BitVector = {
    var res = new BitVector(size)
    res
    var div = (size-1)/8
    val residu = (size-1)%8
    if (residu > 0) div = div + 1
    for (nbyte <- 0 to div-1) {
    res.bits(nbyte) = bits(nbyte)
    }
    res    
  }
  
  /** Reescrivim la funcio toString per representar el vector
   * com un String d'uns i zeros. */
  override def toString= {
  	(List.range(0,getSize()).map(x=>if (this.get(x)) 1 else 0 )).mkString
  }
}