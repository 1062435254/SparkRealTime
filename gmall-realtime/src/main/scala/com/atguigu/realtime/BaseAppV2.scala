package com.atguigu.realtime

import com.atguigu.realtime.utils.{MyKafka, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

trait BaseAppV2 {
    val master: String
    val appName: String
    val sec: Int
    val topic: Set[String]
    val initGroupId: String

    def main(args: Array[String]): Unit = {

        val streamingContext: StreamingContext = new StreamingContext(new SparkConf().setMaster(master).setAppName(appName), Seconds(sec))
        val topics = topic
        val groupId = initGroupId
        val fromOffset = OffsetManager.readOffset(groupId, topics)
        println("初始化偏移量" + fromOffset)
        //为了保证每个topic的数据可以擦除，需要根据topic来创建offsets
        val offsets: Map[String, ListBuffer[OffsetRange]] = topics
            .map(tp => {
                (tp, ListBuffer[OffsetRange]())
            })
            .toMap

        val inputDS: Map[String, DStream[ConsumerRecord[String, String]]] = MyKafka
            .getStreamFromKafka(streamingContext, topics, groupId, fromOffset, false)
            .map {
                case (topic, stream) => {
                    val result = stream
                        .transform(rdd => {
                            //每一个topic的数据
                            offsets(topic).clear()

                            val ranges = rdd
                                .asInstanceOf[HasOffsetRanges]
                                .offsetRanges
                            offsets(topic) ++= ranges
                            rdd
                        })
                    (topic,result)
                }
            }

        run(streamingContext, inputDS, offsets)
        //启动StreamingContext
        streamingContext.start()
        //阻止主线程退出
        streamingContext.awaitTermination()
    }

    def run(streamingContext: StreamingContext,
            ds: Map[String, DStream[ConsumerRecord[String, String]]] ,
            offset: Map[String, ListBuffer[OffsetRange]])



    def saveOffset(offsets: Map[String, ListBuffer[OffsetRange]]): Unit = {

        val offset = offsets.values.reduce(_ ++ _)
        OffsetManager.saveOffsets(initGroupId,offset)
    }

}
