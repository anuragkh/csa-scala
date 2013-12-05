package csa

import org.apache.spark.util._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import collection.mutable.ArrayBuffer
import collection.immutable.HashMap
import wavelettree.WaveletTree
import dictionary.Dictionary
import bitmap.BMArray
import bitmap.BitMap

class CSA(master:String, fileName:String) extends Serializable {

    var BPos:Dictionary = _
    var SA:BMArray = _
    var SAinv:BMArray = _
    var count:Long = _
    val sampleRate = 1 << 5
    var waveletMap:collection.Map[Int,wavelettree.WaveletTree] = _
    var columnOffset:Array[Long] = _
    var rowOffset:Array[Long] = _
    var cellOffset:Array[Seq[Long]] = _
    var nerOffset:Array[Seq[Int]] = _
    var necOffset:Array[Seq[Int]] = _
    var contextSizes:Array[Int] = _
    var partitionOffset:Array[Int] = _
    var contextId:HashMap[Int, Int] = new HashMap[Int, Int]
    var contexts:Array[Int] = _
    var alphabetId:HashMap[Int, Int] = new HashMap[Int, Int]
    var alphabets:Array[Int] = _

    var sigmaMap:HashMap[Int, Int] = _
    var numContexts:Int = _
    var numAlphabet:Int = _

    build

    def build() = {

        // Set Spark System properties
        System.setProperty("spark.executor.memory", "2g")

        // Create New SparkContext
        val sc = new SparkContext(master, "CSA", System.getenv("SPARK_HOME"))
        
        // Set sampling rate for SA
        val sampleRate = (1 << 5)
        
        // Read SA values from file -- TODO: construct SA array
        val saValues = sc.textFile(fileName)

        // Get count
        count = saValues.count
        
        // Get initial SA Map
        val saMap = saValues.map(line => line.split(" "))
                            .map(line => (line(1).toLong, 
                                         (line(0).toLong, 
                                          line(1).toLong, 
                                          line(2).toInt, 
                                          line(3).toInt * 256 + line(4).toInt)))

        // Sample SA values
        val saSampled = saMap.map(t => (t._2._1, t._2._2))
                             .filter(_._2 % sampleRate == 0)
                             .map(t => (t._1, (t._2 / sampleRate))).collect
                             .sortWith((a, b) => (a._1 < b._1))

        println("Collected SA sampling information")

        SA = new BMArray(saSampled.size, Helper.intLog2(saSampled.size + 1))
        var BPosInit = new BitMap(count)
        for(i <- 0 to saSampled.size - 1) {
          SA.setVal(i, saSampled(i)._2)
          BPosInit.setBit(saSampled(i)._1.toInt)
        }
        
        // These are currently local to the master -- Distribute
        println("Created sampled SA")

        //BPos = new Dictionary(BPosInit)
        //BPosInit = null
        
        println("Created BPos bitmap")

        SAinv = new BMArray(saSampled.size, Helper.intLog2(saSampled.size + 1))
        for(i <- 0 to saSampled.size - 1) {
          SAinv.setVal(SA.getVal(i).toInt, i)
        }
        
        println("Created sampled SAinv")

        val psiMap = saMap.flatMap(t => 
                                    Iterator(t,
                                             (((t._1 - 1 + count) % count).toLong,
                                             (t._2._1, (-1).toLong, -1, -1))
                                             )
                                   )
        val psiRDD = psiMap.reduceByKey((a, b) => 
                                            if(a._2 == -1) 
                                                (b._1, a._1, b._3, b._4) 
                                            else (a._1, b._1, a._3, a._4))
                           .map(t => (t._2._1, (t._2._2, t._2._3, t._2._4)))

        // At this point, (K, V) = (i, (psi(i), sigma, context))

        columnOffset = psiRDD.map(t => (t._2._2, t._1))
                             .reduceByKey((a, b) => if(a < b) a else b)
                             .map(t => t._2)
                             .collect.sorted

        numAlphabet = columnOffset.size
        println("Created Column Offsets, size = " + numAlphabet)
        
        rowOffset = psiRDD.map(t => (t._2._3, 1))
                          .reduceByKey((a:Int, b:Int) => a + b)
                          .collect
                          .sortWith((a, b) => a._1 < b._1)
                          .map(t => t._2.toLong)

        for(i <- 1 to rowOffset.size - 1)
            rowOffset(i) += rowOffset(i - 1)
        for(i <- rowOffset.size - 1 to 1 by -1)
            rowOffset(i) = rowOffset(i - 1)

        rowOffset(0) = 0
        numContexts = rowOffset.size
        println("Created Row Offsets, size = " + numContexts)
        
        cellOffset = psiRDD.map(t => ((t._2._2, t._2._3), t._1))
                                .groupByKey.map(t => (t._1._1, t._2.min))
                                .groupByKey.map(t => t._2.sorted)
                                .collect.sortWith((x, y) => x(0) < y(0))
        
        cellOffset = cellOffset.map(list =>  {
                                    val min = list.min
                                    list.map(element => element - min)
                                })

        println("Created Cell Offsets")

        contexts = psiRDD.map(t => (t._2._3, 0))
                         .groupByKey.map(_._1)
                         .collect.sorted

        for(i <- 0 to contexts.size - 1)
            contextId += contexts(i) -> i
            
        println("Created Contexts Array and Map")
        
        alphabets = psiRDD.map(t => (t._2._2, 0))
                          .groupByKey.map(_._1)
                          .collect.sorted

        for(i <- 0 to alphabets.size - 1)
            alphabetId += alphabets(i) -> i

        println("Created Alphabets Array and Map")

        necOffset = psiRDD.map(t => 
                                ((alphabetId(t._2._2), contextId(t._2._3)), 0))
                          .reduceByKey(_ + _)
                          .map(t => t._1)
                          .groupByKey.collect.sortWith((a, b) => a._1 < b._1)
                          .map(t => t._2.sorted)
        println("Created Non-Empty Cells Column-Wise")
        
        nerOffset = psiRDD.map(t => 
                                ((contextId(t._2._3), alphabetId(t._2._2)), 0))
                          .reduceByKey(_ + _)
                          .map(t => t._1)
                          .groupByKey.collect.sortWith((a, b) => a._1 < b._1)
                          .map(t => t._2.sorted)
        println("Created Non-Empty Cells Row-Wise")
        
        contextSizes = nerOffset.map(t => t.size)
        for(i <- 1 to contextSizes.size - 1) {
            contextSizes(i) += contextSizes(i - 1)
        }

        val psiContexts = psiRDD.map(t => (t._2._3, (t._2._1, t._2._2)))
                                .groupByKey
                                .map(t => (t._1, createContextMap(t._2.sortWith((x, y) => (x._2 < y._2)).asInstanceOf[ArrayBuffer[(Long, Int)]])))

        // At this point, (K, V) = (context, Array(i, psi(i), sigma).sortedBy(sigma))
        waveletMap = psiContexts.map(t => (t._1, new WaveletTree(t._2)))
                                    .collectAsMap
                                    
        println("Collected wavelet map; construction complete!")
//        waveletMap = waveletRDD.mapPartitions(data => Iterator(createHashMap(data))).cache
//        val boundaryKeys = waveletRDD.mapPartitionsWithIndex{case(idx, data) => Iterator(data.minBy(_._1)._1)}
//        partitionOffset = boundaryKeys.collect.sorted
//        
//        println("Created Wavelet Tree RDD")

    }
    
    def createHashMap(data:Iterator[(Int, WaveletTree)]) = {
        var dataMap = new HashMap[Int, WaveletTree]
        while(data.hasNext) dataMap += data.next
        dataMap
    }
    
    def createContextMap(map:ArrayBuffer[(Long, Int)]) = {
        var contextMap = new ArrayBuffer[(Long, Int)]
        var previousSigma = map(0)._2
        var minValue = map.minBy(_._1)._1
        var j = 0
        for(i <- 0 to map.size - 1) {
            if(map(i)._2 != previousSigma) j += 1
            contextMap += new Tuple2(map(i)._1 - minValue.toLong, j)
            previousSigma = map(i)._2
        }
        contextMap
    }
    
    def binSearch(V:Seq[Long], i:Long):Long = {
        var sp = 0
        var ep = V.size - 1
        while(sp <= ep) {
            val m = (sp + ep) / 2
            if(V(m) == i) return (m + 1)
            else if(i < V(m)) ep = m - 1
            else sp = m + 1
        }
        ep + 1
    }
    
    def binSearch(V:Seq[Int], i:Int):Int = {
        var sp = 0
        var ep = V.size - 1
        while(sp <= ep) {
            val m = (sp + ep) / 2
            if(V(m) == i) return (m + 1)
            else if(i < V(m)) ep = m - 1
            else sp = m + 1
        }
        ep + 1
    }
    

    def lookupPsi(i:Long) = {
        //println("Lookup Psi, i = " + i)
        val col = binSearch(columnOffset, i.toInt) - 1
        //println("col = " + col)
        val colNum = columnOffset(col.toInt).toInt
        //println("colNum = " + colNum)
        val rowNum = binSearch(cellOffset(col.toInt), (i.toInt - colNum)) - 1
        //println("rowNum = " + rowNum)
        val cellPos = (i - colNum - cellOffset(col.toInt)(rowNum.toInt)).toInt
        //println("cellPos = " + cellPos)
        val row = necOffset(col.toInt)(rowNum.toInt).toInt
        //println("row = " + row)
        val rowOff = rowOffset(row).toInt
        //println("rowOff = " + rowOff)
        //val rowSizePrev = (if(row > 0) contextSizes(row - 1) else 0).toInt
        //println("rowSizePrev = " + rowSizePrev)
        val rowPosCur = binSearch(nerOffset(row), col.toInt) - 1
        //println("rowPosCur = " + rowPosCur)
        //val rowSizeCur = (if(row > 0) contextSizes(row) - contextSizes(row - 1) else contextSizes(0)).toInt
        //println("rowSizeCur = " + rowSizeCur)
        val contextPos = rowPosCur
        //println("contextPos = " + contextPos)
        rowOff + waveletMap(contexts(row)).lookup(contextPos, cellPos)
  }

    def lookupSA(idx:Long) = {
        var v = 0
        var i = idx
        while(BPos.rank1(i.toInt) - BPos.rank1(i.toInt - 1) == 0) {
          i = lookupPsi(i)
          v += 1
        }
        val r = BPos.rank1(i.toInt)
        val a = SA.getVal(if(r == 0) count.toInt - 1 else r.toInt - 1) * sampleRate
        if(a < v) (a + count - v) else a - v
      }

    def lookupSAinv(i:Long) = {
        var v = i % sampleRate
        val a = SAinv.getVal(i.toInt / sampleRate)
        var pos = BPos.select1(a.toInt)
        while(v != 0) {
          pos = lookupPsi(pos)
          v -= 1
        }
        pos
      }
}