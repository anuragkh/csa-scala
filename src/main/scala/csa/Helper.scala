package csa

object Helper {
    val two32 = (1L << 32)
    def intLog2(inp:Long) = math.ceil(math.log(inp) / math.log(2)).toInt
}
