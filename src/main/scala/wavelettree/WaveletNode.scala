package wavelettree

import dictionary.Dictionary
import bitmap.BitMap
import scala.collection.mutable.ArrayBuffer

class WaveletNode(start:Byte, end:Byte, contextMap:ArrayBuffer[(Long, Int)]) extends Serializable {
  var left:WaveletNode = null
  var right:WaveletNode = null
  val id:Byte = math.min(contextMap((contextMap.size - 1) / 2)._2, end - 1).toByte
  var D:Dictionary = _
  build

  private def build() = {
    //println("start = " + start + " end = " + end)
    var contextMapLeft:ArrayBuffer[(Long, Int)] = new ArrayBuffer[(Long, Int)]
    var contextMapRight:ArrayBuffer[(Long, Int)] = new ArrayBuffer[(Long, Int)]
    //var valuesLeft:ArrayBuffer[Long] = new ArrayBuffer[Long]
    //var valuesRight:ArrayBuffer[Long] = new ArrayBuffer[Long]
    //var cellsLeft:ArrayBuffer[Byte] = new ArrayBuffer[Byte]
    //var cellsRight:ArrayBuffer[Byte] = new ArrayBuffer[Byte]

    var B = new BitMap(contextMap.size)
    //println("Bitmap size = " + B.size + " = " + values.size)
    for(i <- 0 to (contextMap.size - 1)){
      //println("i = " + i + " val = " + values(i) + " cell = " + cells(i))
      if(contextMap(i)._2 > id && contextMap(i)._2 <= end){
        B.setBit(contextMap(i)._1.toInt)
      }
    }

    //println("Created BitMap.")
    D = new Dictionary(B)
    B = null
    //println("BitMap size = " + D.size)
    //println("Dictionary size = " + D.B.size)

    for(i <- 0 to (contextMap.size - 1)) {
      //println("i = " + i + " val = " + contextMap(i)._1 + " cell = " + contextMap(i)._2)
      //println("i = " + i + " val = " + values(i) + " cell = " + cells(i))
      if(contextMap(i)._2 > id && contextMap(i)._2 <= end) {
        contextMapRight += new Tuple2(D.rank1(contextMap(i)._1.toInt) - 1, contextMap(i)._2)
        //cellsRight += cells(i)
      } else {
        contextMapLeft += new Tuple2(D.rank0(contextMap(i)._1.toInt) - 1, contextMap(i)._2)
        //cellsLeft += cells(i)
      }
    }

    left = if(start == id) null else new WaveletNode(start, id, contextMapLeft)
    contextMapLeft = null
    //cellsLeft = null
    right = if(id + 1 == end) null else new WaveletNode((id + 1).toByte, end, contextMapRight)
    contextMapRight = null
    //cellsRight = null
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
