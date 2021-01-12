package com.atguigu.realtime.utils

import java.lang

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object MyKafka {
    var kafkaParams: Map[String, Object] = Map[String, Object](
        "bootstrap.servers" -> "hadoop162:9092,hadoop163:9092,hadoop164:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "auto.offset.reset" -> "latest", //开始消费的时候，如有上次的位置就从上次的位置消费，没有就从头消费
        "enable.auto.commit" -> (true: java.lang.Boolean) //改为手动提交，自动提交偏移量
    )

    /**
     * 分两种情况，一种是一个消费者消费一个topic
     * 另一个情况是一个消费者消费多个topic
     *
     * @param streamContext
     * @param topics  消费的topic集合
     * @param groupId 消费这group id
     * @param isOne   是否消费一个
     * @return
     */
    def getStreamFromKafka(streamContext: StreamingContext,
                           topics: Set[String],
                           groupId: String,
                           isOne: Boolean = true) = {

        kafkaParams += "group.id" -> groupId

        if (isOne) {
            val value = KafkaUtils.createDirectStream[String, String](
                streamContext,
                PreferConsistent,
                Subscribe[String, String](topics, kafkaParams)
            )
            Map("all" -> value)

        }
        else {
            null
        }
    }


    def getStreamFromKafka(streamContext: StreamingContext,
                           topics: Set[String],
                           groupId: String,
                           offset: collection.Map[TopicPartition, Long],
                           isOne: Boolean) = {

        kafkaParams += "group.id" -> groupId
        kafkaParams += ("enable.auto.commit" -> (false: java.lang.Boolean)) //设定自动提交


        if (isOne) {
            val value = KafkaUtils.createDirectStream[String, String](
                streamContext,
                PreferConsistent,
                Subscribe[String, String](topics, kafkaParams, offset)
            )
            Map("all" -> value)

        }
        else {
            //offsets包含六个topic信息
            topics
                .map(topic => {
                    val value = KafkaUtils.createDirectStream[String, String](
                        streamContext,
                        PreferConsistent,
                        Subscribe[String, String](Set(topic), kafkaParams, offset.filter(_._1.topic() == topic))
                    )
                    (topic, value)
                })
                .toMap
        }
    }

    val map: Map[String, Object] = Map[String, Object](
        "bootstrap.servers" -> "hadoop162:9092,hadoop163:9092,hadoop164:9092",
        "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
        "enable.idempotent" -> (true: lang.Boolean)
    )

    import scala.collection.JavaConverters._

    def getProducter() = {
        new KafkaProducer[String, String](map.asJava)
    }

    def sendStreamToKafka(topicAndData: Iterator[(String, String)]) = {

        //创建生产者
        val producter = getProducter()
        //写数据
        topicAndData.foreach {
            case (topic, data) => {
                producter.send(new ProducerRecord(topic, data))
            }
        }
        //关闭
        producter.close()
    }


}
