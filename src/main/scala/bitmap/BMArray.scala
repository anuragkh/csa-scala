package bitmap

class BMArray(n_bma:Int, bits_bma:Int) extends BitMap(n_bma.toLong * bits_bma.toLong) {
    
    val n = n_bma
    val bits = bits_bma

    def getVal(i:Int) = {
        var value = 0.toLong
        val s = (i.toLong * bits.toLong)
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
    def setVal(i:Int, value:Long) = {
        val s = (i.toLong * bits.toLong)
        val e = s + (bits - 1).toLong
        if((s / 64L) == (e / 64L)) {
            bitmap((s / 64L).toInt) |= (value << (63L - e % 64))
        } else {
            bitmap((s / 64L).toInt) |= (value >>> (e % 64 + 1))
            bitmap((e / 64L).toInt) |= (value << (63L - e % 64))
        }
    }
}
