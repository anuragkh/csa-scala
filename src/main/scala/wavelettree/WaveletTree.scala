package wavelettree

import scala.collection.mutable.ArrayBuffer

class WaveletTree(contextMap:ArrayBuffer[(Long, Int)]) extends Serializable {
  
  val maxCellId = contextMap.maxBy(_._2)._2.toByte
  val root = if(maxCellId == 0) null else new WaveletNode(0, maxCellId, contextMap)

  def lookup(contextPos:Int, cellPos:Int) = if(root == null) cellPos else root.lookup(contextPos, cellPos, 0, maxCellId)
}