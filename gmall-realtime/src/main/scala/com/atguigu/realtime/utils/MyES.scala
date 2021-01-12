package com.atguigu.realtime.utils

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client._
import io.searchbox.core.{Bulk, Index}

import scala.collection.JavaConverters._

object MyES {
    //创建客户端
    val clientFactory = new JestClientFactory
    val list = List("http://hadoop162:9200", "http://hadoop163:9200", "http://hadoop164:9200")
    val config = new HttpClientConfig.Builder(list.asJava)
        //连接超时，读超时，多线程，总连接数
        .connTimeout(10 * 1000)
        .readTimeout(10 * 1000)
        .multiThreaded(true)
        .maxTotalConnection(100)
        .build()

    clientFactory.setHttpClientConfig(config)

    //向es中写数据
    def insertSingle(indexStr: String, source:Object,id: String = null): Unit = {
        val client: JestClient = clientFactory.getObject
        val myIndex = new Index.Builder(source)
            .index(indexStr)
            .`type`("_doc")
            .id(id)
            .build()

        client.execute(myIndex)

        //关闭客户端
        client.close()
    }

    //批次写数据
    def insertBulk(indexStr:String,sources:Iterator[Object]): Unit ={
        val client = clientFactory.getObject
        //创建bulk的对象
        val bulkBulider = new Bulk.Builder()
            .defaultIndex(indexStr)
            .defaultType("_doc")

        //如果使用map的情况下没有返回值，是不会执行这个语句的
        sources.foreach{
            case (id:String,source) =>
                bulkBulider.addAction(new Index.Builder(source).id(id).build())
            case source =>
                bulkBulider.addAction(new Index.Builder(source).build())
        }
        client.execute(bulkBulider.build())
        client.close()
    }

    def main(args: Array[String]): Unit = {
        val source =
            """{"name": "zs","age": 20,"girls":["a", "b", "c"]}"""

        val source1 =
            """{"name": "zs","age": 20,"girls":["a", "b", "c"]}"""
        //insertSingle("test1",source)
        insertBulk("test",Iterator(source,("abc",new user("zhangshan",1))))
    }
    class user(name:String,age:Int)
}
