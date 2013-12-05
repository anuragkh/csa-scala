import dictionary._
import bitmap._
val B = new BitMap(100)
for(i <- 0 to 99) if((new util.Random()).nextBoolean) B.setBit(i)
for(i <- 0 to 99) print(B.getBit(i))
val D = new Dictionary(B)
for(i <- 0 to 100) println("i = " + i + " r = " + D.rank1(i))

import wavelettree.WaveletTree
import collection.mutable.ArrayBuffer
val contextMap = new ArrayBuffer[(Long, Int)]
for(i <- 0 to 99) contextMap += new Tuple2((99 - i).toLong, i)
val wtree = new WaveletTree(contextMap)