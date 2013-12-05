package wavelettree

import dictionary.Dictionary
import bitmap.BitMap
import scala.collection.mutable.ArrayBuffer

class WaveletNode(start:Byte, end:Byte, values:ArrayBuffer[Long], cells:ArrayBuffer[Byte]) {
  var left:WaveletNode = null
  var right:WaveletNode = null
  val id:Byte = math.min(cells((values.size - 1) / 2), end - 1).toByte
  var D:Dictionary = _
  build

  private def build() = {
    //println("start = " + start + " end = " + end)
    var valuesLeft:ArrayBuffer[Long] = new ArrayBuffer[Long]
    var valuesRight:ArrayBuffer[Long] = new ArrayBuffer[Long]
    var cellsLeft:ArrayBuffer[Byte] = new ArrayBuffer[Byte]
    var cellsRight:ArrayBuffer[Byte] = new ArrayBuffer[Byte]

    var B = new BitMap(values.size)
    //println("Bitmap size = " + B.size + " = " + values.size)
    for(i <- 0 to (values.size - 1)){
      //println("i = " + i + " val = " + values(i) + " cell = " + cells(i))
      if(cells(i) > id && cells(i) <= end){
        B.setBit(values(i).toInt)
      }
    }

    //println("Created BitMap.")
    D = new Dictionary(B)
    B = null

    for(i <- 0 to (values.size - 1)) {
      //println("i = " + i + " val = " + values(i) + " cell = " + cells(i))
      if(cells(i) > id && cells(i) <= end) {
        valuesRight += D.rank1(values(i).toInt) - 1
        cellsRight += cells(i)
      } else {
        valuesLeft += D.rank0(values(i).toInt) - 1
        cellsLeft += cells(i)
      }
    }

    left = if(start == id) null else new WaveletNode(start, id, valuesLeft, cellsLeft)
    valuesLeft = null
    cellsLeft = null
    right = if(id + 1 == end) null else new WaveletNode((id + 1).toByte, end, valuesRight, cellsRight)
    valuesRight = null
    cellsRight = null
  }

  def lookup(contextPos:Int, cellPos:Int, start:Int, end:Int):Int = {
    //println("start = " + start + " end = " + end)
    if(contextPos > id && contextPos <= end) {
      //println("r")
      if(right == null) {
        //println("leaf=>" + cellPos + " propagates " + D.select1(cellPos).toInt)
        //D.printD
        D.select1(cellPos).toInt
      }
      else {
        val l = right.lookup(contextPos, cellPos, id + 1, end)
        //println("non-leaf=>" + l + " propagates " + D.select1(l).toInt)
        //D.printD
        D.select1(l).toInt
      }
    } else {
      //println("l")
      if(left == null) {
        //println("leaf=>" + cellPos + " propagates " + D.select0(cellPos).toInt)
        //D.printD
        D.select0(cellPos).toInt
      }
      else {
        val l = left.lookup(contextPos, cellPos, start, id)
        //println("non-leaf=>" + l + " propagates " + D.select0(l).toInt)
        //D.printD
        D.select0(l).toInt
      }
    }
  }
}
