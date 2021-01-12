package com.atguigu.realtime.utils

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

object OffsetManager {
    def saveOffsets(groupId: String, offset: ListBuffer[OffsetRange]) = {
        val key = s"offset_${groupId}"
        val client = MyRedis.getClient()
        val value = offset.map(off => {
            val field = s"${off.topic}:${off.partition}"
            val offset = off.untilOffset
            field -> offset.toString
        }).toMap
            .asJava
        client.hmset(key, value)
        println("读到的偏移量：" + value)
        client.close()
    }

    def readOffset(groupId: String, topics: Set[String]): collection.Map[TopicPartition, Long] = {
        val key = s"offset_${groupId}"
        val client = MyRedis.getClient()
        val result = client
            .hgetAll(key)
            .asScala
            .map {
                case (field: String, value: String) =>
                    val split = field.split(":")
                    new TopicPartition(split(0), split(1).toInt) -> value.toLong
            }

        client.close()
        result
    }
}
