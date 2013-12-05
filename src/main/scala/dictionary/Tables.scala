package dictionary

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap

object Tables {

    var decodeTable = new Array[Array[Int]](17)
    var encodeTable = new Array[HashMap[Int, Int]](17)
    var C16 = new Array[Int](17)
    var offbits = new Array[Byte](17)
    var smallrank = Array.tabulate[Byte](65536, 16)((i, j) => java.lang.Long.bitCount(i >>> (15 - j)).toByte)
    var q = new Array[Int](17)
    
    C16(0) = 2
    offbits(0) = 1
    C16(1) = 16
    offbits(1) = 4
    C16(2) = 120
    offbits(2) = 7
    C16(3) = 560
    offbits(3) = 10
    C16(4) = 1820
    offbits(4) = 11
    C16(5) = 4368
    offbits(5) = 13
    C16(6) = 8008
    offbits(6) = 13
    C16(7) = 11440
    offbits(7) = 14
    C16(8) = 12870
    offbits(8) = 14
    
    for(i <- 0 to 16) {
        if(i > 8) {
            C16(i) = C16(16 - i)
            offbits(i) = offbits(16 - i)
        }
        decodeTable(i) = new Array[Int](C16(i))
        encodeTable(i) = new HashMap[Int, Int]()
    }
    
    q(16) = 1
    
    for(i <- 0 to 65535) {
        val p = java.lang.Long.bitCount(i)
        decodeTable(p % 16)(q(p)) = i
        encodeTable(p % 16) += i -> q(p)
        q(p) += 1
    }

}
