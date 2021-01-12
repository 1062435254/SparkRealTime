package com.atguigu.realtime.ods

import com.atguigu.realtime.BaseAppV1
import com.atguigu.realtime.utils.MyKafka
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.{JsonMethods, Serialization}

import scala.collection.mutable.ListBuffer

object MaxwellOdsApp extends BaseAppV1 {
    //从kafka钟消费数据后，对数据进行过滤  分流得到自己想要的数据后，继续存入kafka
    override val master: String = "local[2]"
    override val appName: String = "MaxwellOdsApp"
    override val sec: Int = 5
    override val topic: Set[String] = Set("maxwell_gmall_db")
    override val initGroupId: String = "MaxwellOdsApp"

    val tableNames = List(
        "order_info",
        "order_detail",
        "user_info",
        "base_province",
        "base_category3",
        "sku_info",
        "spu_info",
        "base_trademark")

    override def run(streamingContext: StreamingContext, ds: DStream[String], offset: ListBuffer[OffsetRange]) = {
        //ds.print()
        ds
            .map(line => {
                implicit val f = org.json4s.DefaultFormats
                val lineValue = JsonMethods.parse(line)
                val jValue = lineValue \ "data"
                //如果jValue是数组的形式，可以通过这种方式得到全部数据

                //解析是把Json的格式转换为对象使用extract
                val tableName = (lineValue \ "table").extract[String]
                val opearte = (lineValue \ "type").extract[String]

                //将JValue序列化成Json格式的字符串
                (tableName, opearte, Serialization.write(jValue))
            })

            .filter {
                case (tableName, opearte, str) => {
                    tableNames.contains(tableName) && (opearte == "insert" || opearte == "update" || "bootstrap-insert" == opearte)
                }
            }

            //将数据过滤后存入kafka，每个table一个topic
            .foreachRDD(rdd => {
                rdd.foreachPartition(it => {
                    val sendData = it
                        .filter {
                            case (tableName, opearte, data) =>
                                tableName != "order_info" || opearte == "insert"
                        }
                        .map {
                            case (tableName, opearte, data) => ("ods_" + tableName, data)
                        }
                    MyKafka.sendStreamToKafka(sendData)
                })
                saveOffset(offset)
            })

    }
}
