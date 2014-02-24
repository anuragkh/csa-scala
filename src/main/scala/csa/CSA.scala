package csa

import org.apache.spark.util._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.RangePartitioner
import org.apache.spark.HashPartitioner
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.hadoop.fs.Path
import org.apache.spark.storage.StorageLevel

import java.io.File
import java.io.PrintWriter
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.HashMap
import wavelettree.WaveletTree
import dictionary.Dictionary
import bitmap.BMArray
import bitmap.BitMap
import scala.collection.mutable.ListBuffer
import scala.collection.Map
import scala.collection.immutable.Vector
import scala.util.Random

class CSA(master:String, fileName:String, numTasks:String, memory:String) extends Serializable {

    var BPos:Dictionary = _
    var SA:BMArray = _
    var SAinv:BMArray = _
    var count:Long = _
    var bCount:Broadcast[Long] = null
    val sampleRate = 1 << 5               
    var waveletMap:Map[Int,WaveletTree] = _
    var columnOffset:Array[Long] = _
    var rowOffset:Array[Long] = _
    var cellOffset:Array[Seq[Long]] = _
    var nerOffset:Array[Seq[Int]] = _
    var necOffset:Array[Seq[Int]] = _
    var partitionOffset:Array[Int] = _
    var contextId:HashMap[Int, Int] = new HashMap[Int, Int]
    var contexts:Array[Int] = _
    var alphabetId:HashMap[Int, Int] = new HashMap[Int, Int]
    var alphabets:Array[Int] = _
    var numContexts:Int = _
    var numAlphabet:Int = _
    var partSizes:Array[Long] = _

    build

    def build() = {

        // Set Spark System properties
        System.setProperty("spark.executor.memory", memory)
        System.setProperty("spark.default.parallelism", numTasks)
        System.setProperty("spark.akka.frameSize", "1024");
        // System.setProperty("spark.cores.max", "4")
        System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        System.setProperty("spark.kryo.registrator", "Registrator")
        System.setProperty("spark.kryoserializer.buffer.mb", "24")
        System.setProperty("spark.storage.memoryFraction", "0.3")
        System.setProperty("spark.speculation", "true")
        System.setProperty("spark.speculation.interval", "60000")
        System.setProperty("spark.shuffle.consolidateFiles", "true")

        // Create New SparkContext
        val sc = new SparkContext(master, "CSA", System.getenv("SPARK_HOME"), 
                                  List("target/scala-2.9.3/csa_2.9.3-1.0.jar"))
        //val reporter = new org.apache.spark.scheduler.StatsReportListener()
        //sc.addSparkListener(reporter)

        // Set sampling rate for SA
        val sampleRate = (1 << 5)
        
        // Start timer!
        val startTimeBuild = System.currentTimeMillis
        
        // Get count
        val configuration:Configuration = new Configuration
        var fileSystem:FileSystem = null

        if (fileName.startsWith("hdfs://")) {
            configuration.addResource(
                new Path("/root/ephemeral-hdfs/conf/core-site.xml"))
            configuration.addResource(
                new Path("/root/ephemeral-hdfs/conf/hdfs-site.xml"))
            fileSystem = FileSystem.get(configuration)
        } else {
            fileSystem = FileSystem.getLocal(configuration).getRawFileSystem
        }

        count = fileSystem.getFileStatus(new Path(fileName)).getLen + 1
        bCount = sc.broadcast(count)

        fileSystem.close

        println("Count = " + count)

        println("Num tasks = " + numTasks.toInt);
        val textRDD = sc.hadoopFile(fileName,
                                    classOf[TextInputFormat],
                                    classOf[LongWritable],
                                    classOf[Text],
                                    numTasks.toInt)
                        .map(t => (t._1.get, t._2.toString))
                        .flatMap(splitLineWithIndex)
                        .sortByKey(true)
                        .persist(StorageLevel.MEMORY_ONLY_SER)
                         
        // count = textRDD.count + 1
        // bCount = sc.broadcast(count)

        textRDD.saveAsTextFile(fileName + "_textRDD");
                         
        // partSizes = textRDDInit.mapPartitionsWithIndex{
        //                             case(idx, data) => Iterator((idx, data.size))
        //                             }
        //                         .collect
        //                         .sortBy(_._1)
        //                         .map(_._2.toLong)
        
        // println("Initial partSizes")
        // partSizes.foreach(println)                           
        // for(i <- 1 to partSizes.size - 1)
        //     partSizes(i) += partSizes(i - 1)

        // println("Final partSizes")
        // partSizes.foreach(println)
            
        // val textRDD = textRDDInit.mapPartitionsWithIndex(mapToNewIndex)
        //                          .persist(StorageLevel.MEMORY_ONLY_SER)

        // println("Count = " + textRDD.count)


        val suffRDD = makeSuffixArray(textRDD).persist(StorageLevel.MEMORY_ONLY_SER)
        println("Size of SA = " + suffRDD.count)

        val tripletRDD = textRDD.flatMap(t => Iterator((t._1, (t._2.toInt, 0)), 
                                                       ((t._1 - 1 + bCount.value) % bCount.value, (t._2.toInt, 1)), 
                                                       ((t._1 - 2 + bCount.value) % bCount.value, (t._2.toInt, 2))))
                                .groupByKey
                                .mapValues(t => {
                                        val _t = t.sortBy(_._2).map(_._1)
                                        (_t(0), _t(1) * 256 + _t(2))
                                    })

        // tripletRDD.saveAsTextFile(fileName + "_triplet")
        val saMap = suffRDD.map(_.swap)
                           .join(tripletRDD)
                           .map(t => (t._1, (t._2._1, t._1, t._2._2._1, t._2._2._2)))
                           .persist(StorageLevel.MEMORY_ONLY_SER)

        println("SA Map count = " + saMap.count)

        // saMap.saveAsTextFile(fileName + "_samap")

//        val suffRDD = textRDD.flatMap(t => mapToPrev(t, 1024))
//                             .groupByKey
//                             .map(t => 
//                                (t._2.sortBy(_._1).map(e => e._2)
//                                                        .mkString, t._1))
//                             .sortByKey(true)
//                             .persist(StorageLevel.MEMORY_ONLY_SER)
//        
//        println("Printing SuffixRDD")
//        suffRDD.saveAsTextFile(fileName + "_files")
//
//        partSizes = suffRDD.mapPartitionsWithIndex{
//                                    case(idx, data) => Iterator((idx, data.size))
//                                    }
//                           .collect
//                           .sortBy(_._1)
//                           .map(t => t._2)
//
//        for(i <- 1 to partSizes.size - 1)
//            partSizes(i) = partSizes(i - 1)
//            
//        val saValues = suffRDD.mapPartitionsWithIndex(mapToNewIndex)
//                              .persist(StorageLevel.MEMORY_ONLY_SER)
//        saValues.saveAsTextFile(fileName + "_savalues")
//
//        // val saValues = sc.textFile(fileName)
//
//        // Get initial SA Map
//        // val saMap = saValues.map(line => line.split(" "))
//        //                     .map(line => (line(1).toLong, 
//        //                                  (line(0).toLong, 
//        //                                   line(1).toLong, 
//        //                                   line(2).toInt, 
//        //                                   line(3).toInt * 256 + line(4).toInt)))
//
//        val saMap = saValues.map(t => (t._2, t))
//                            .persist(StorageLevel.MEMORY_ONLY_SER)
//
       // Sample SA values
       var saSampled = saMap.map(t => (t._2._1, t._2._2))
                            .filter(_._2 % sampleRate == 0)
                            .map(t => (t._1, (t._2 / sampleRate)))
                            .persist(StorageLevel.MEMORY_ONLY_SER)
                            //.collect
                            //.sortWith((a, b) => (a._1 < b._1))

       println("Collected SA sampling information")
       
       // // These are currently local to the master -- Distribute
       // val sampledSize = saSampled.size
       // SA = new BMArray(sampledSize, Helper.intLog2(sampledSize + 1))
       // var BPosInit = new BitMap(count)
       // for(i <- 0 to saSampled.size - 1) {
       //     SA.setVal(i, saSampled(i)._2)
       //     BPosInit.setBit(saSampled(i)._1.toInt)
       // }
       // println("Created sampled SA")
       
       // // Cleanup!
       // saSampled = null
       // System.gc

       // BPos = new Dictionary(BPosInit)
       // println("Created BPos bitmap")

       // // Cleanup again!
       // BPosInit = null
       // System.gc

       val psiRDD = saMap.flatMap(t => 
                                   Iterator(t,
                                            (((t._1 - 1L + bCount.value) % bCount.value),
                                            (t._2._1, (-1).toLong, -1, -1))
                                            )
                                  )
                         .reduceByKey((a, b) => 
                                           if(a._2 == -1) 
                                               (b._1, a._1, b._3, b._4) 
                                           else (a._1, b._1, a._3, a._4))
                         .map(t => (t._2._1, (t._2._2, t._2._3, t._2._4)))
                         .persist(StorageLevel.MEMORY_ONLY_SER)

        saMap.unpersist(true);

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

       // I am here
       
       contexts = psiRDD.map(t => (t._2._3, 0))
                        .groupByKey.map(_._1)
                        .collect.sorted

       for(i <- 0 to contexts.size - 1)
           contextId += contexts(i) -> i
           
       println("Created Contexts Array and Map")
       
       alphabets = psiRDD.map(t => (t._2._2, 0))
                         .groupByKey.map(_._1)
                         .collect.sorted

       println("Displaying alphabetId mapping")
       for(i <- 0 to alphabets.size - 1)
           alphabetId += alphabets(i) -> i
       
       val bAlphabetId = sc.broadcast(alphabetId)
       val bContextId = sc.broadcast(contextId)

       println("Created Alphabets Array and Map")

       necOffset = psiRDD.map(t => 
                               ((bAlphabetId.value(t._2._2), 
                                 bContextId.value(t._2._3)), 
                                 0))
                         .reduceByKey(_ + _)
                         .map(t => t._1)
                         .groupByKey.collect.sortWith((a, b) => a._1 < b._1)
                         .map(t => t._2.sorted)

       println("Created Non-Empty Cells Column-Wise")
       
       nerOffset = psiRDD.map(t => 
                               ((bContextId.value(t._2._3), 
                                 bAlphabetId.value(t._2._2)), 
                                 0))
                         .reduceByKey(_ + _)
                         .map(t => t._1)
                         .groupByKey.collect.sortWith((a, b) => a._1 < b._1)
                         .map(t => t._2.sorted)
       println("Created Non-Empty Cells Row-Wise")

       // psiRDD(K, V) = (i, (psi(i), sigma, context))
       val waveletRDD = psiRDD.map(t => (t._2._3, (t._2._1, t._2._2)))
                              .groupByKey
                              .map(t => 
                                   (t._1, 
                                   createContextMap(t._2.sortWith((x, y) => (x._2 < y._2))
                                                        .asInstanceOf[ArrayBuffer[(Long, Int)]])))
                              .map(t => (t._1, new WaveletTree(t._2)))
                              .persist(StorageLevel.MEMORY_ONLY_SER)
                               
//        waveletMap = waveletRDD.collectAsMap
       
       println("Number of wavelet trees: " + waveletRDD.count)
       println("Created Wavelet RDD")
       // SAinv = new BMArray(sampledSize, Helper.intLog2(sampledSize + 1))
       // for(i <- 0 to sampledSize - 1)
       //   SAinv.setVal(SA.getVal(i).toInt, i)
       // println("Created sampled SAinv")

       println("Construction Complete!")

//        waveletMap = waveletRDD.mapPartitions(data => Iterator(createHashMap(data))).persist(StorageLevel.MEMORY_ONLY_SER)
//        val boundaryKeys = waveletRDD.mapPartitionsWithIndex{case(idx, data) => Iterator(data.minBy(_._1)._1)}
//        partitionOffset = boundaryKeys.collect.sorted
//        
//        println("Created Wavelet Tree RDD")

        // Stop timer!
        val endTimeBuild = System.currentTimeMillis
        val buildTime = endTimeBuild - startTimeBuild
        
        println("Build time = " + buildTime + "ms")
    }
    
    // def makeSuffixArrayNew(textRDD:RDD[(Long, Char)]) = {

    //     val len = 1

    //     var substrRDD = textRDD.mapValues(_.toInt.toLong)
    //                            .sortByKey(true)
    //                            .persist(StorageLevel.MEMORY_ONLY_SER)

    //     var substrCount = textRDD.values.distinct.count
    //     println("Initial distinct substring count = " + substrCount)

    //     val indexCount = textRDD.count
    //     println("Index count = " + indexCount)


    // }
    
    def makeSuffixArray(textRDD:RDD[(Long, Char)]) = {
        var len = 1
        var substrRDD = textRDD.mapValues(_.toInt.toLong)
                               .sortByKey(true)
                               .persist(StorageLevel.MEMORY_ONLY_SER)
        
        var substrCount = textRDD.values.distinct.count
        println("Initial Distinct Substring count = " + substrCount)

        val indexCount = textRDD.count

        while(substrCount < indexCount) {
            println("Length: " + len)
            //substrRDD = substrRDD.sortByKey(true)
            //println("RDD TYPE: " + substrRDD.getClass.getSimpleName)
            //println("Partitioner: " + substrRDD.partitioner)
            
            val mergeAndSwapRDD = substrRDD.flatMap(t => mapToLenIndex(t, len))
                                        .reduceByKey(reduceToSingleTuple)
                                        .map(t => ((t._2._1, t._2._2, ((new Random()).nextInt % 256).toChar), t._1))
                                        .sortByKey(true)
                                        .persist(StorageLevel.MEMORY_ONLY_SER)
            
            substrRDD.unpersist(true)
            
            var distinctCounts = mergeAndSwapRDD.mapPartitionsWithIndex(getDistinctCounts, true)
                                        .collect
                                        .sortBy(_._1)
            println("distinctCounts size = " + distinctCounts.size)
            
            var beginOffsets = new Array[Long](distinctCounts.size)
            beginOffsets(0) = 0
            for(i <- 1 to distinctCounts.size - 1) {
                val check = (distinctCounts(i - 1)._4 == distinctCounts(i)._3)
                val off = if(check) (distinctCounts(i - 1)._2 - 1) 
                          else distinctCounts(i - 1)._2
                beginOffsets(i) = beginOffsets(i - 1) + off
            }

            substrCount = beginOffsets(beginOffsets.size - 1) + 
                          distinctCounts(beginOffsets.size - 1)._2

            distinctCounts = null
            
            substrRDD = mergeAndSwapRDD.mapPartitionsWithIndex((i, d) => 
                                            remapIndices(i, d, beginOffsets), true)
                                        .map(_.swap)
                                        .persist(StorageLevel.MEMORY_ONLY_SER)
            mergeAndSwapRDD.unpersist(true)
            
            println("Distinct substring count = " + substrCount)
            println("Index count = " + indexCount)
            len = if(substrCount != indexCount) len * 2 else len
        }

        substrRDD.map(_.swap).sortByKey(true)
    
    }

    def mapToLenIndex(t:(Long, Long), len:Int):Iterator[(Long, (Long, Long))] = {
        Iterator((t._1, (t._2, 0L)), 
                 ((t._1 - len + bCount.value) % bCount.value, (t._2, 1L)))
    }

    def reduceToSingleTuple(a:(Long, Long), b:(Long, Long)) = {
        if(a._2 == 0L) (a._1, b._1)
        else (b._1, a._1)
    }

    def getDistinctCounts(i:Int, d:Iterator[((Long, Long, Char), Long)]) = {
        var count = 0L
        var prvSubstr:Tuple3[Long, Long, Char] = (-1L, -1L, 0.toChar)
        var i = 0
        var firstSubstr:Tuple3[Long, Long, Char] = (-1L, -1L, 0.toChar)

        while(d.hasNext) {
            val substr = d.next._1
            if(i == 0) firstSubstr = substr
            if(prvSubstr._1 != substr._1 || prvSubstr._2 != substr._2)
                count += 1
            prvSubstr = substr
            i += 1
        }
        Iterator((i, count, firstSubstr, prvSubstr))
    }

    def remapIndices(i:Int, d:Iterator[((Long, Long, Char), Long)], beginOffsets:Seq[Long]) = {
        val dataArray = new ListBuffer[(Long, Long)]()
        var count = -1L
        var prvSubstr:Tuple3[Long, Long, Char] = (-1L, -1L, 0.toChar)
        val offset = beginOffsets(i)
        while(d.hasNext) {
            val elem = d.next
            val substr = elem._1
            if(prvSubstr._1 != substr._1 || prvSubstr._2 != substr._2)
                count += 1
            prvSubstr = substr
            dataArray += ((offset + count, elem._2))
        }
        dataArray.iterator
    }

    // def mapToNewIndex(idx:Int, data:Iterator[(Long, Char)]) = {
    //     val dataArray = new ListBuffer[(Long, Char)]()

    //     var i = if(idx == 0) 0 
    //             else partSizes(idx - 1)
    //     while(data.hasNext) {
    //         val curData = data.next
    //         val c = curData._2
    //         dataArray += ((i, c))
    //         i += 1
    //     }
    //     if(i == bCount.value - 1) {
    //         println("Nulling")
    //         dataArray += ((i, '\0'))
    //     }
    //     dataArray.iterator
    // }

    def splitLineWithIndex(line:(Long, String)) = {
        val lineoffset = line._1
        val linestring = line._2
        val size = linestring.length + 1

        var lineArray = new Array[(Long, Char)](size)
        for(i <- 0 to linestring.length - 1)
            lineArray(i) = (lineoffset + i, linestring(i))

        if(linestring.length + lineoffset == bCount.value - 1) {
            println("Nulling!")
            lineArray(linestring.length) = (lineoffset + linestring.length, 1.toChar)
        } else {
            // println("Not Nulling: " + bCount.value + ", " + (linestring.length + lineoffset))
            lineArray(linestring.length) = (lineoffset + linestring.length, '\n')
        }
        lineArray.iterator
    }

    def mapToPrev(data:(Long, Char), L:Int) = {
        var mappedData = new Array[(Long, (Int, Char))](L)
        for(i <- 0 to L - 1)
            mappedData(i) = ((data._1 - i + count) % count, (i, data._2))
        mappedData.iterator
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
    
    def getCinvIndex(i:Long):Int = {
        var sp = 0
        var ep = columnOffset.size - 1
        while(sp <= ep) {
            val m = (sp + ep) / 2
            if(columnOffset(m) == i) return m
            else if(i < columnOffset(m)) ep = m - 1
            else sp = m + 1
        }
        ep
    }

    def binSearchPsi(value:Long, start:Long, end:Long, flag:Boolean):Long = {
        
        var sp = start
        var ep = end

        while(sp <= ep) {
            val m = (sp + ep) / 2
            val psiVal = lookupPsi(m)
            if(psiVal == value) return m
            else if(value < psiVal) ep = m - 1
            else sp = m + 1
        }
        
        if(flag) ep else sp
    }
    
    def getRangeBckSlow(p:String):(Long, Long) = {
        var range = (0L, -1L)
        var m = p.length
        var sp = 0L
        var ep = 0L
        var c1 = 0L
        var c2 = 0L
        
        if(alphabetId.contains(p(m - 1))) {
            sp = columnOffset(alphabetId(p(m - 1)))
            ep = columnOffset(alphabetId(p(m - 1)) + 1) - 1
        } else return range
        
        for(i <- m - 2 to 0 by -1) {
            if(alphabetId.contains(p(i))) {
                c1 = columnOffset(alphabetId(p(i)))
                c2 = columnOffset(alphabetId(p(i)) + 1) - 1
            } else return range
            sp = binSearchPsi(sp, c1, c2, false)
            ep = binSearchPsi(ep, c1, c2, true)
            if(sp > ep) return range
        }
        (sp, ep)
    }
    
    def getRangeBck(p:String):(Long, Long) = {
        var m = p.length
        if(m <= 2) getRangeBckSlow(p)

        var range = (0L, -1L)
        var a = alphabetId(p(m - 3))
        var c = contextId(p(m - 2) * 256 + p(m - 1))
        var startOff = binSearch(necOffset(a), c) - 1
        var sp = columnOffset(a) + cellOffset(a)(startOff)
        var ep = columnOffset(a) + cellOffset(a)(startOff + 1) - 1
        var c1 = 0L
        var c2 = 0L

        for(i <- m - 4 to 0 by -1) {
            if(alphabetId.contains(p(i))) {
                a = alphabetId(p(i))
                c = contextId(p(i + 1) * 256 + p(i + 2))
                startOff = binSearch(necOffset(a), c) - 1
                c1 = columnOffset(a) + cellOffset(a)(startOff)
                c2 = columnOffset(a) + cellOffset(a)(startOff + 1) - 1
            } else return range
            sp = binSearchPsi(sp, c1, c2, false)
            ep = binSearchPsi(ep, c1, c2, true)
            if(sp > ep) return range
        }
        (sp, ep)
    }
    
    def count(p:String) = {
        val range = getRangeBck(p)
        range._2 - range._1 + 1
    }
    
    def locate(p:String) = {
        val range = getRangeBck(p)
        val size = range._2 - range._1 + 1
        if(size <= 0) null
        else Array.tabulate(size.toInt)(i => lookupSA(range._1 + i))
    }
    
    def extract(i:Long, j:Long) = {
        var s = lookupSAinv(i)
        val size = (j - i + 1).toInt
        var text = new Array[Char](size)
        for(k <- 0 to size - 1) {
            text(k) = alphabets(getCinvIndex(s)).toChar
            s = lookupPsi(s)
        }
        text
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
