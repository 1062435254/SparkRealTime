package com.atguigu.realtime

import com.atguigu.realtime.utils.{MyKafka, OffsetManager}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait BaseAppV1 {
    val master :String
    val appName :String
    val sec:Int
    val topic:Set[String]
    val initGroupId:String

    def main(args: Array[String]): Unit = {

        val streamingContext: StreamingContext = new StreamingContext(new SparkConf().setMaster(master).setAppName(appName), Seconds(sec))

        //先从redis读取上次消费的位置
        //储存offset格式  key:offset_groupId,  value: (topic_p0,100)
        val topics = topic
        val groupId = initGroupId
        val fromOffset = OffsetManager.readOffset(groupId, topics)
        println("初始化偏移量" + fromOffset)
        //从kafka中取出的偏移量
        val offset: ListBuffer[OffsetRange] = ListBuffer[OffsetRange]()
        //使用kafkaUtil获取一个流
        val inputDS = MyKafka.getStreamFromKafka(streamingContext, topics, groupId, fromOffset,true)("all")
            .transform(rdd => {
                //每五秒清空一次
                offset.clear()
                //一个分区用一个offset拼接
                val value = rdd.asInstanceOf[HasOffsetRanges]
                    .offsetRanges
                offset ++= value
                rdd
            })
        //inputDS.map(_.value()).print()
        val ds = inputDS.map(_.value())

        run(streamingContext,ds,offset)
        //启动StreamingContext
        streamingContext.start()
        //阻止主线程退出
        streamingContext.awaitTermination()
    }

    def run(streamingContext:StreamingContext,ds: DStream[String],offset:ListBuffer[OffsetRange])

    def saveOffset(offsets: ListBuffer[OffsetRange]): Unit = {
        OffsetManager.saveOffsets(initGroupId, offsets)
    }

}
