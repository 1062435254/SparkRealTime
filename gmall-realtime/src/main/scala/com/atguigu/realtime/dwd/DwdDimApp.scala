package com.atguigu.realtime.dwd

import com.atguigu.realtime.BaseAppV2
import com.atguigu.realtime.bean.{ProvinceInfo, UserInfo}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.JsonMethods
import org.apache.phoenix.spark._

import scala.collection.mutable.ListBuffer

object DwdDimApp extends BaseAppV2 {
    override val master: String = "local[2]"
    override val appName: String = "DwdDimApp"
    override val sec: Int = 5
    override val topic: Set[String] = Set("ods_base_province", "ods_user_info")
    override val initGroupId: String = "DwdDimApp"


    def saveToPhoenix[T <: Product](offset: Map[String, ListBuffer[OffsetRange]],
                                    dsStream: DStream[ConsumerRecord[String, String]],
                                    tableName: String,
                                    cols: Seq[String])(implicit mf: scala.reflect.Manifest[T]) = {
        dsStream
            .map(it => {

                val line = it.value()
                val jValue = JsonMethods.parse(line)

                implicit val f = org.json4s.DefaultFormats
                jValue.extract[T]
            })
            .foreachRDD(rdd => {
                rdd.saveToPhoenix(
                    tableName,
                    cols,
                    zkUrl = Option("hadoop162,hadoop163,hadoop164:2181")
                )
                saveOffset(offset)
            })
    }


    override def run(streamingContext: StreamingContext, dsMap: Map[String, DStream[ConsumerRecord[String, String]]], offset: Map[String, ListBuffer[OffsetRange]]): Unit = {
        //ds("ods_user_info").map(_.value()).print()

        saveToPhoenix[UserInfo](
            offset,
            dsMap("ods_user_info"),
            "GMALL_USER_INFO",
            Seq("ID", "USER_LEVEL", "BIRTHDAY", "GENDER", "AGE_GROUP", "GENDER_NAME"),
        )

        saveToPhoenix[ProvinceInfo](
            offset,
            dsMap("ods_base_province"),
            "GMALL_PROVINCE_INFO",
            Seq("ID", "NAME", "AREA_CODE", "ISO_CODE"),
        )
    }

}

