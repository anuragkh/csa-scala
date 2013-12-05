import org.apache.spark.util.*

object CSA {
    def main(args: Array[String]) {
        if (args.length != 1) {
            System.err.println("Usage: CSA <master> <filename>")
            System.exit(1)
        }
        val sc = new SparkContext(args(0), "CSA", System.getenv("SPARK_HOME"))
        val fileName = args(1)
        val saValues = sc.textFile(fileName)
        val count = saValues.count
        val saMap = saValues.map(line => line.split(" ")).map(line => (line(1).toLong, (line(0).toLong, line(1).toLong, line(2).toInt, line(3).toInt * 256 + line(4).toInt)))
        val psiMap = saMap.flatMap(t => List(t, (((t._1 - 1 + count) % count).toLong, (t._2._1, (-1).toLong, -1, -1))))
        val psiRDD = psiMap.reduceByKey((a, b) => if(a._2 == -1) (b._1, a._1, b._3, b._4) else (a._1, b._1, a._3, a._4)).map(t => (t._2._1, (t._2._2, t._2._3, t._2._4)))
        val psiContexts = psiRDD.map(t => (t._2._3, (t._1, t._2._1, t._2._2))).groupByKey.take(1)
        for(i <- 0 to psiContexts._2.size - 1) println(psiContexts._2._1)
    }
}
