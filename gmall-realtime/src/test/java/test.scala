import com.atguigu.realtime.bean.StartupLog
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods


object test {
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  private val streamingContext = new StreamingContext(new SparkContext(new SparkConf().setMaster("local[2]").setAppName("test")),Seconds(2))
  val topics = Array("topicA", "topicB")
  val stream = KafkaUtils.createDirectStream[String, String](
    streamingContext,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )

  stream.map(record => (record.key, record.value))


  /*
    在scala中转换json格式的数据
   */
  def main(args: Array[String]): Unit = {
    val str = "{\"common\":{\"ar\":\"110000\",\"ba\":\"iPhone\",\"ch\":\"Appstore\",\"md\":\"iPhone Xs\",\"mid\":\"mid_44\",\"os\":\"iOS 13.3.1\",\"uid\":\"254\",\"vc\":\"v2.1.132\"},\"start\":{\"entry\":\"icon\",\"loading_time\":1980,\"open_ad_id\":4,\"open_ad_ms\":4851,\"open_ad_skip_ms\":0},\"ts\":1609778531000}"
    val jv = JsonMethods.parse(str)
    val jvCommon = jv \ "common"
    val jvST = jv \ "ts"

    implicit val f = org.json4s.DefaultFormats
    //将Jstring隐式转换成string
    /*
      在scala中 <:  表示B必须是JValue的子类型
      def merge[B <: JValue, R <: JValue](other: B)(implicit instance: MergeDep[A, B, R]): R =
            Merge.merge(json, other)(instance)
            }
    val log = jvCommon.merge(JObject("ts" -> jvST)).extract[StartupLog]

    println(log.ar)*/

  }



}
