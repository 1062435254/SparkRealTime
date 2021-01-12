package com.atguigu.realtime.app

import java.time.LocalDate

import com.atguigu.realtime.bean.StartupLog
import com.atguigu.realtime.utils.{MyES, MyKafka, MyRedis}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.Formats
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods

object Dauapp {

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
  def distinct2(startupLogStream: DStream[StartupLog]) = {
    println("----进入distinct----")
    // 问题: 性能问题. 每条数据创建一个到redis的连接, 对性能有影响
    startupLogStream.filter(log => {
      println("-----进入filter----")
      // 把这条日志中的设备id存入到redis的set中,如果返回1这条记录保留, 如果返回0就过滤掉
      val client = MyRedis.getClient()
      // set集合: 应该每天一个, 存储当天的启动的设备id
      val setKey = s"dau:uids:${log.logDate}"
      val r = 1 == client.sadd(setKey, log.mid)
      client.close()
      r
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

  def main(args: Array[String]): Unit = {

    //创建一个StreamingContext
    val streamingContext: StreamingContext = new StreamingContext(new SparkConf().setMaster("local[2]").setAppName("Dauapp"), Seconds(3))

    //使用kafkaUtil获取一个流
    val inputDS = MyKafka.getStreamFromKafka(streamingContext, Set("gmall_start_topic"), "Dauapp", true)("all")
    //inputDS.map(_.value()).print()
    val ds = inputDS.map(_.value())

    //对流进行各种操作
    //将数据封装到样例类
    val startLogVal = parseToStartupLog(ds)
    //startLogVal.print()


    //对数据去重，对每个设备，只保留当天第一次启动的数据
    //val disStartVal = distinct(StartLogVal)
    val disStartVal: DStream[StartupLog] = distinct(startLogVal)

    val indexStr = s"gmall_dau_info_${LocalDate.now().toString}"

/*   disStartVal.foreachRDD(rdd => {
     rdd.foreachPartition((it: Iterator[StartupLog]) => {
      //连接es
      //写入数据
      //关闭e
      println(indexStr)
      //将数据定义为（设备id_登入时间，数据）
      MyES.insertBulk(indexStr,it.map(log => (s"${log.mid}_${log.logDate}",log)))
    })
  })
*/

  //数据写入到外部存储es
  disStartVal.saveToES(indexStr)

  //启动StreamingContext
  streamingContext.start()
  //阻止主线程退出
  streamingContext.awaitTermination()
}


implicit class RichES(stream:DStream[StartupLog]){
  def saveToES(indexStr:String) ={
    stream.foreachRDD(rdd => {
      rdd.foreachPartition((it: Iterator[StartupLog]) => {
        //连接es
        //写入数据
        //关闭e
        println(indexStr)
        //将数据定义为（设备id_登入时间，数据）
        MyES.insertBulk(indexStr,it.map(log => (s"${log.mid}_${log.logDate}",log)))
      })
    })
  }
}
}
