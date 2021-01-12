package com.atguigu.realtime.dwd

import com.atguigu.realtime.BaseAppV1
import com.atguigu.realtime.bean.OrderInfo
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.CustomSerializer
import org.json4s.JsonAST.{JDouble, JInt, JLong, JString}
import org.json4s.jackson.JsonMethods

import scala.collection.mutable.ListBuffer

object DwdOrderInfo extends BaseAppV1 {
    override val master: String = "local[2]"
    override val appName: String = "DwdOrderInfo"
    override val sec: Int = 5
    override val topic: Set[String] = Set("ods_order_info")
    override val initGroupId: String = "DwdOrderInfo"

    //Do not know how to convert JString(4667) into long
    val toLong = new CustomSerializer[Long](formats => ( {
        case JString(s) => s.toLong
        case JInt(num) => num.toLong
    }, {
        case n: Long => JLong(n)
    }
    ))

    //Do not know how to convert JString(4667) into long
    val toDouble = new CustomSerializer[Double](formats => ( {
        case JString(s) => s.toDouble
        case JDouble(d) => d.toDouble
    }, {
        case n: Double => JDouble(n)
    }
    ))



    override def run(streamingContext: StreamingContext, ds: DStream[String], offset: ListBuffer[OffsetRange]): Unit = {
        //把数据解析成样例类
        ds.map(line => {
            implicit val f = org.json4s.DefaultFormats + toLong + toDouble
            JsonMethods.parse(line).extract[OrderInfo]
        })
            .print()
        //判断首单业务

        //与维度表进行JOin，补齐相关维度，
    }
}
