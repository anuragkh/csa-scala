import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class Registrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
        kryo.register(classOf[String])
        kryo.register(classOf[bitmap.BitMap])
        kryo.register(classOf[bitmap.BMArray])
        kryo.register(classOf[dictionary.Dictionary])
        kryo.register(classOf[wavelettree.WaveletNode])
        kryo.register(classOf[wavelettree.WaveletTree])
        kryo.register(classOf[csa.CSA])
    }
}
