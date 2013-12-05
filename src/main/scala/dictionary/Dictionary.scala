package dictionary

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
import csa.Helper
import bitmap.BitMap

class Dictionary(BM:BitMap) extends Serializable {
    val size = BM.size
    var rankL3 = new Array[Long](((size / (1L << 32)) + 1).toInt);
    var rankL12 = new Array[Long](((size / 2048) + 1).toInt);
    var posL3 = new Array[Long](((size / (1L << 32)) + 1).toInt);
    var posL12 = new Array[Long](((size / 2048) + 1).toInt);
    var B:BitMap = _
    
    build

    private def build() = {
        rankL3(0) = 0
        rankL12(0) = 0
        posL3(0) = 0
        posL12(0) = 0
        var sumL1 = 0L
        var sumPosL1 = 0L
        var flag = 0
        var compSize = 0L
        var dict:ListBuffer[Int] = new ListBuffer[Int]()
        var count = 0L
        var rankL1 = 0L
        var rankL2 = 0L
        var posL1 = 0L
        var posL2 = 0L
        var p = 0
        
        //println("Size : " + (size / 2048) + 1)
        //println("Rank size: " + rankL12.size)
        for(i <- 0L to (size - 1)) {
            //println("i = " + i)
            if(i % (1L << 32) == 0) {
                rankL3((i / (1L << 32)).toInt) = count
                posL3((i / (1L << 32)).toInt) = compSize
            }
            if(i % 2048 == 0) {
                rankL2 = count - rankL3((i / (1L << 32)).toInt)
                posL2 = compSize - posL3((i / (1L << 32)).toInt)
                rankL12((i / 2048).toInt) = rankL2 << 32
                posL12((i / 2048).toInt) = posL2 << 31
                flag = 0
                sumL1 = 0
                sumPosL1 = 0
            }
            if(i % 512 == 0) {
                rankL1 = count - rankL2 - sumL1
                posL1 = compSize - posL2 - sumPosL1
                sumL1 += rankL1
                sumPosL1 += posL1
                if(flag != 0) {
                    rankL12((i / 2048).toInt) |= (rankL1 << (32 - flag * 10))
                    posL12((i / 2048).toInt) |= (posL1 << (31 - flag * 10))
                }
                flag += 1
            }
            if(BM.getBit(i.toInt) == 1) {
                count += 1
            }
            if(i % 64 == 0) {
                p = java.lang.Long.bitCount((BM.bitmap((i / 64).toInt) >> 48).toInt & 65535) % 16
                compSize += Tables.offbits(p)
                dict += p
                dict += Tables.encodeTable(p)((BM.bitmap((i / 64).toInt) >> 48).toInt & 65535)
                
                p = java.lang.Long.bitCount((BM.bitmap((i / 64).toInt) >> 32).toInt & 65535) % 16
                compSize += Tables.offbits(p)
                dict += p
                dict += Tables.encodeTable(p)((BM.bitmap((i / 64).toInt) >> 32).toInt & 65535)
                
                p = java.lang.Long.bitCount((BM.bitmap((i / 64).toInt) >> 16).toInt & 65535) % 16
                compSize += Tables.offbits(p)
                dict += p
                dict += Tables.encodeTable(p)((BM.bitmap((i / 64).toInt) >> 16).toInt & 65535)
                
                p = java.lang.Long.bitCount(BM.bitmap((i / 64).toInt) & 65535).toInt % 16
                compSize += Tables.offbits(p)
                dict += p
                dict += Tables.encodeTable(p)(BM.bitmap((i / 64).toInt).toInt & 65535)
                
                compSize += 16;
            }
        }

        B = new BitMap(compSize)
        var numBits = 0
        for(i <- 0 to (dict.size - 1)) {
            if(i % 2 == 0) {
                B.setValPos(numBits, dict(i), 4)
                numBits += 4
            } else {
                B.setValPos(numBits, dict(i), Tables.offbits(dict(i - 1)))
                numBits += Tables.offbits(dict(i - 1))
            }
        }
        dict = null
    }
    private def GETRANKL2(n:Long) = (n >>> 32)
    private def GETPOSL2(n:Long) = (n >>> 31)
    private def GETRANKL1(n:Long, i:Int) = (((n & 0xffffffff) >>> (32 - i * 10)) & 0x3ff)
    private def GETPOSL1(n:Long, i:Int) = (((n & 0x7fffffff) >>> (31 - i * 10)) & 0x3ff)
    def select1(i:Int) = {
        var value = (i + 1).toLong
        var sp:Int = 0
        var ep:Int = (size / Helper.two32).toInt
        var m = 0
        var r = 0L
        var pos = 0L
        var blockClass = 0
        var blockOffset = 0
        var sel = 0L
        var lastblock = 0
        
        while(sp <= ep) {
          //println(sp)
          //println(ep)
            m = (sp + ep) / 2
            r = rankL3(m)
            if(value > r)
                sp = m + 1
            else
                ep = m - 1
        }
        
        ep = math.max(ep, 0)
        sel += ep * Helper.two32
        value -= rankL3(ep)
        pos += posL3(ep)
        sp = (ep * (Helper.two32 / 2048)).toInt
        ep = math.min(((ep + 1) * (Helper.two32 / 2048L).toInt), math.ceil(size.toDouble / 2048.0).toInt - 1)
        
        while(sp <= ep) {
            m = (sp + ep) / 2
            r = GETRANKL2(rankL12(m))
            if(value > r)
                sp = m + 1
            else
                ep = m - 1
        }
        
        ep = math.max(ep, 0)
        sel += ep * 2048
        value -= GETRANKL2(rankL12(ep))
        pos += GETPOSL2(posL12(ep))
        
        r = GETRANKL1(rankL12(ep), 1)
        if(sel + 512 < size && value > r) {
            pos += GETPOSL1(posL12(ep), 1)
            value -= r
            sel += 512
            r = GETRANKL1(rankL12(ep), 2)
            if(sel + 512 < size && value > r) {
                pos += GETPOSL1(posL12(ep), 2)
                value -= r
                sel += 512
                r = GETRANKL1(rankL12(ep), 3)
                if(sel + 512 < size && value > r) {
                    pos += GETPOSL1(posL12(ep), 3)
                    value -= r
                    sel += 512
                }
            }
        }

        breakable {
            while(true) {
                blockClass = B.getValPos(pos.toInt, 4).toInt
                val blockBits = Tables.offbits(blockClass)
                pos += 4
                blockOffset = (if(blockClass == 0) B.getBit(pos.toInt) * 16 else 0).toInt
                pos += blockBits

                if (value <= ((blockClass + blockOffset))) {
                    pos -= (4 + blockBits)
                    break
                }
                value -= (blockClass + blockOffset)
                sel += 16
            }
        }
        
        blockClass = B.getValPos(pos.toInt, 4).toInt
        pos += 4;
        blockOffset = B.getValPos(pos.toInt, Tables.offbits(blockClass)).toInt
        lastblock = Tables.decodeTable(blockClass)(blockOffset)
        
        var count = 0
        breakable {
            for(i <- 0 to 16) {
                if(((lastblock >>> (15 - i)) & 1) == 1)
                    count += 1
                if(count == value) {
                    sel += i
                    break
                }
            }
        }
        sel
    }

    def select0(i:Int) = {
        var value = (i + 1).toLong
        var sp:Int = 0
        var ep:Int = (size / Helper.two32).toInt
        var m = 0
        var r = 0L
        var pos = 0L
        var blockClass = 0
        var blockOffset = 0
        var sel = 0L
        var lastblock = 0
        
        while(sp <= ep) {
            m = (sp + ep) / 2
            r = m * Helper.two32 - rankL3(m)
            if(value > r)
                sp = m + 1
            else
                ep = m - 1
        }
        
        ep = math.max(ep, 0)
        sel += ep * Helper.two32
        value -= (ep * Helper.two32 - rankL3(ep))
        pos += posL3(ep)
        sp = (ep * (Helper.two32 / 2048)).toInt
        ep = math.min(((ep + 1) * (Helper.two32 / 2048L).toInt), math.ceil(size.toDouble / 2048.0).toInt - 1)
        
        while(sp <= ep) {
            m = (sp + ep) / 2
            r = m * 2048 - GETRANKL2(rankL12(m))
            if(value > r)
                sp = m + 1
            else
                ep = m - 1
        }
        
        ep = math.max(ep, 0)
        sel += ep * 2048
        value -= (ep * 2048 - GETRANKL2(rankL12(ep)))
        pos += GETPOSL2(posL12(ep))
        
        r = 512 - GETRANKL1(rankL12(ep), 1)
        if(sel + 512 < size && value > r) {
            pos += GETPOSL1(posL12(ep), 1)
            value -= r
            sel += 512
            r = 512 - GETRANKL1(rankL12(ep), 2)
            if(sel + 512 < size && value > r) {
                pos += GETPOSL1(posL12(ep), 2)
                value -= r
                sel += 512
                r = 512 - GETRANKL1(rankL12(ep), 3)
                if(sel + 512 < size && value > r) {
                    pos += GETPOSL1(posL12(ep), 3)
                    value -= r
                    sel += 512
                }
            }
        }

        breakable {
            while(true) {
                blockClass = B.getValPos(pos.toInt, 4).toInt
                val blockBits = Tables.offbits(blockClass)
                pos += 4
                blockOffset = (if(blockClass == 0) B.getBit(pos.toInt) * 16 else 0).toInt
                pos += blockBits

                if (value <= (16 - (blockClass + blockOffset))) {
                    pos -= (4 + blockBits)
                    break
                }
                value -= (16 - (blockClass + blockOffset))
                sel += 16
            }
        }
        
        blockClass = B.getValPos(pos.toInt, 4).toInt
        pos += 4;
        blockOffset = B.getValPos(pos.toInt, Tables.offbits(blockClass)).toInt
        lastblock = Tables.decodeTable(blockClass)(blockOffset)
        
        var count = 0
        breakable {
            for(i <- 0 to 16) {
                if(((lastblock >>> (15 - i)) & 1) == 0)
                    count += 1
                if(count == value) {
                    sel += i
                    break
                }
            }
        }
        sel
    }
    def rank1(i:Int) = {
        var res = 0L
        if(i >= 0) {
            val idxL3 = (i / Helper.two32).toInt
            val idxL2 = i / 2048
            var idxL1 = i % 512
            val rem = (i % 2048) / 512
            var blockClass = 0
            var blockOffset = 0
            
            res = rankL3(idxL3) + GETRANKL2(rankL12(idxL2))
            var pos = posL3(idxL3) + GETPOSL2(posL12(idxL2))
            
            //println("rank_init = " + res)
            //println("pos_init = " + pos)
            rem match {
                case 1 =>   {
                                //println("Case 1 Matched!")
                                res += GETRANKL1(rankL12(idxL2), 1)
                                pos += GETPOSL1(posL12(idxL2), 1)
                            }
                case 2 =>   {
                                //println("Case 2 Matched!")
                                res += GETRANKL1(rankL12(idxL2), 1) + GETRANKL1(rankL12(idxL2), 2)
                                pos += GETPOSL1(posL12(idxL2), 1) + GETPOSL1(posL12(idxL2), 2)
                            }
                case 3 =>   {
                                //println("Case 3 Matched!")
                                res += GETRANKL1(rankL12(idxL2), 1) + GETRANKL1(rankL12(idxL2), 2) + GETRANKL1(rankL12(idxL2), 3)
                                pos += GETPOSL1(posL12(idxL2), 1) + GETPOSL1(posL12(idxL2), 2) + GETPOSL1(posL12(idxL2), 3)
                            }
                case 0 =>   {
                                //println("Case 0 Matched!")
                            }
            }
            
            //println("pos = " + pos + " idxL1 = " + idxL1)
            while(idxL1 >= 16) {
                blockClass = B.getValPos(pos.toInt, 4).toInt
                pos += 4
                blockOffset = if(blockClass == 0) B.getBit(pos.toInt).toInt * 16 else 0
                pos += Tables.offbits(blockClass)
                res += blockClass + blockOffset
                idxL1 -= 16
            }
            
            blockClass = B.getValPos(pos.toInt, 4).toInt
            pos += 4
            blockOffset = B.getValPos(pos.toInt, Tables.offbits(blockClass)).toInt
            val lastblock = Tables.decodeTable(blockClass)(blockOffset)
            //println("blockclass = " + blockClass)
            //println("blockOffset = " + blockOffset)
            //println("lastblock = " + Tables.decodeTable(blockClass)(blockOffset))
            //println("lastblock = ")
            //printBlock(lastblock)
            //println
            //println("offset = " + idxL1)
            //println("value = " + Tables.smallrank(lastblock)(idxL1))
            res += Tables.smallrank(Tables.decodeTable(blockClass)(blockOffset))(idxL1)
        }
        res
    }
    def rank0(i:Int) = (i.toLong - rank1(i) + 1L)
    def printBlock(block:Int) = for(i <- 0 to 15) print((block >>> (15 - i)) & 1)
    def printD() = { for(i <- 0 to size.toInt - 1) print(rank1(i) - rank1(i - 1))}
}
