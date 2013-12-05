package bitmap

import scala.util.control.Breaks._

class BitMap(bm_size:Long) extends Serializable {
    val size = bm_size
    var bitmap = new Array[Long]((size / 64).toInt + 1)

    def getBit(i:Int) = ((bitmap(i / 64) >>> (63L - i)) & 1L)
    def setBit(i:Int) = { bitmap(i / 64) |= (1L << (63L - i)) }
    def getValPos(pos:Int, bits:Int) = {
        var value = 0.toLong
        val s = pos.toLong
        val e = s + (bits - 1).toLong

        if ((s / 64L) == (e / 64L)) {
            value = bitmap((s / 64L).toInt) << (s % 64)
            value = value >>> (63 - e % 64 + s % 64)
        } else {
            value = bitmap((s / 64L).toInt) << (s % 64)
            value = value >>> (s % 64 - (e % 64 + 1))
            value = value | (bitmap((e / 64L).toInt) >>> (63 - e % 64))
        }
        value
    }
    def setValPos(pos:Int, value:Long, bits:Int) = {
        val s = pos.toLong
        val e = s + (bits - 1).toLong
        if((s / 64L) == (e / 64L)) {
            bitmap((s / 64L).toInt) |= (value << (63L - e % 64))
        } else {
            bitmap((s / 64L).toInt) |= (value >>> (e % 64 + 1))
            bitmap((e / 64L).toInt) |= (value << (63L - e % 64))
        }
    }
    def rank1(i:Int) = {
        var count = 0
        for(k <- 0 to i) if(getBit(k) == 1) count += 1
        count
    }
    def rank0(i:Int) = {
        var count = 0
        for(k <- 0 to i) if(getBit(k) == 0) count += 1
        count
    }
    def select1(i:Int) = {
        var sel = -1
        var count = 0
        breakable {
            for(k <- 0 to size.toInt) {
                if(getBit(k) == 1) count += 1
                if(count == (i + 1)) {
                    sel = k
                    break
                }
            }
        }
        sel
    }
    def select0(i:Int) = {
        var sel = -1
        var count = 0
        breakable {
            for(k <- 0 to size.toInt) {
                if(getBit(k) == 0) count += 1
                if(count == (i + 1)) {
                    sel = k
                    break
                }
            }
        }
        sel
    }
}