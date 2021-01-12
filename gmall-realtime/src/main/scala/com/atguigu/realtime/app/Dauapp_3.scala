package com.atguigu.realtime.app

import java.time.LocalDate

import com.atguigu.realtime.BaseAppV1
import com.atguigu.realtime.app.Dauapp_2.{distinct, parseToStartupLog}
import com.atguigu.realtime.bean.StartupLog
import com.atguigu.realtime.utils.{MyES, MyRedis, OffsetManager}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Dauapp_3 extends BaseAppV1{

    override val master: String = "local[2]"
    override val appName: String = "Dauapp_3"
    override val sec: Int = 5
    override val topic: Set[String] = Set("gmall_start_topic")
    override val initGroupId: String = "Dauapp_3"

    override def run(streamingContext: StreamingContext, ds: DStream[String], offset:ListBuffer[OffsetRange]) = {
        val startLogVal = parseToStartupLog(ds)
        //startLogVal.print()
        //对数据去重，对每个设备，只保留当天第一次启动的数据
        //val disStartVal = distinct(StartLogVal)
        val disStartVal: DStream[StartupLog] = distinct(startLogVal)
        val indexStr = s"gmall_dau_info_${LocalDate.now().toString}"
        //数据写入到外部存储es
        disStartVal.foreachRDD(rdd => {
            rdd.foreachPartition((it: Iterator[StartupLog]) => {

                //将数据定义为（设备id_登入时间，数据）
                MyES.insertBulk(indexStr,it.map(log => (s"${log.mid}_${log.logDate}",log)))
            })

            OffsetManager.saveOffsets(initGroupId,offset)
        })



    }


    def parseToStartupLog(ds: DStream[String]) = {
        ds.map(str => {
            val jv = JsonMethods.parse(str)
            val jvCommand = jv \ "common"
            val jTs = jv \ "ts"
            implicit val f = org.json4s.DefaultFormats
            //JObect传入的形参  type JField = (String, JValue) 是一个字符串加上一个jValue的值
            jvCommand.merge(JObject("ts" -> jTs)).extract[StartupLog]
        })
    }



    def distinct(startLogVal: DStream[StartupLog]) = {

        //由于DS为分区数据，设定为每个分区每个批次建立一次连接
        startLogVal.mapPartitions(values => {
            //建立连接
            //类在没有建立序列化的情况下是不可以在main上打印的
            val redisClient = MyRedis.getClient()
            //println(redisClient.sadd("s5","v1"))

            val result = values.filter(it => {
                //设置key为每天的的日期
                println("-----进入filter-----")
                val setKey = s"dau:uids:${it.logDate}"
                val b = 1 == redisClient.sadd(setKey, it.mid)
                redisClient.expire(setKey, 60 * 24 * 60)
                b
            })

            redisClient.close()
            result
        })

    }


}
