package csa

import collection.mutable.ArrayBuffer

class Psi() extends Serializable {

  val columnOffset = new ArrayBuffer[Long]
  val rowOffset = new ArrayBuffer[Long]
  val cellOffset = new ArrayBuffer[ArrayBuffer[Long]]
  val nerOffset = new ArrayBuffer[ArrayBuffer[Long]]
  val necOffset = new ArrayBuffer[Long]
  val contextSizes = new ArrayBuffer[Long]
  val numContexts = 0
  val numAlphabet = 0

  build

  def build() = {}
  
  def binSearch(V:ArrayBuffer[Long], i:Int) = {
    var sp = 0
    var ep = V.size - 1
    while(sp <= ep) {
      val m = (sp + ep) / 2
      if(V(m) == i) m + 1
      else if(i < V(m)) ep = m - 1
      else sp = m + 1
    }
    ep + 1
  }

  def lookup(i:Long) = {
    val col = binSearch(columnOffset, i.toInt) - 1
    val colNum = columnOffset(col).toInt
    val rowNum = binSearch(cellOffset(col), (i.toInt - colNum)) - 1
    val cellPos = i - colNum - cellOffset(col)(rowNum)
    val row = nerOffset(col)(rowNum).toInt
    val rowOff = rowOffset(row).toInt
    val rowSizePrev = (if(row > 0) contextSizes(row - 1) else 0).toInt
    val rowPosCur = binSearch(necOffset, row * numAlphabet + col)
    val rowSizeCur = (if(row > 0) contextSizes(row) - contextSizes(row - 1) else contextSizes(0)).toInt
    val contextPos = rowPosCur - rowSizePrev - 1
    
    (contextPos, cellPos, rowOff)
  }

}
