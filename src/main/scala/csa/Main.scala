package csa

object Main {
    def main(args: Array[String]) {
        if (args.length != 4) {
            System.err.println("Usage: CSA <master> <filename> <num-tasks> <memory>")
            System.exit(1)
        }
        
        val index = new CSA(args(0), args(1), args(2), args(3))

        println("Finished index construction, exiting!")
        // System.exit(0)
        //var countTime = 0L
        //var locateTime = 0L
        //var extractTime = 0L
        //for(i <- 0 to 1000) {
        //    val countStartTime = System.nanoTime.toLong
        //    val count = index.count(query)
        //    val countEndTime = System.nanoTime.toLong
        //    countTime += (countEndTime - countStartTime)
        //    println("Count = " + count)
            
        //    val locateStartTime = System.nanoTime.toLong
        //    val locate = index.locate(query)
        //    val locateEndTime = System.nanoTime.toLong
        //    locateTime += (locateEndTime - locateStartTime)
        //    println("Locate = " + locate(0))
         
        //    val extractStartTime = System.nanoTime.toLong
        //    val extract = index.extract(locate(0), locate(0) + 1024)
        //    val extractEndTime = System.nanoTime.toLong
        //    extractTime += (extractEndTime - extractStartTime)
        //    println("Extract = " + extract.mkString)
        //}
        
        //println("Count Time = " + countTime.toDouble / 1000.0)
        //println("Locate Time = " + locateTime.toDouble / 1000.0)
        //println("Extract Time = " + extractTime.toDouble / 1000.0)
    }
}
