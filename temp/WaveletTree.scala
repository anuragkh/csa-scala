package wavelettree

import scala.collection.mutable.ArrayBuffer

class WaveletTree(values:ArrayBuffer[Long], cells:ArrayBuffer[Byte]) {
  val root = if(values.size - 1 == 0) null else new WaveletNode(0, cells.max, values, cells)
  val maxCellId = cells.max

  def lookup(contextPos:Int, cellPos:Int) = if(root == null) cellPos else root.lookup(contextPos, cellPos, 0, maxCellId)
}

