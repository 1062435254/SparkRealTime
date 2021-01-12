package com.atguigu.realtime.gmallpublisher.service

object Query {

    val str1 =    """
                   |{
                   |  "query": {
                   |    "match_all": {}
                   |  }
                   |}
                   |""".stripMargin

    val str2: String =
        """{
          |  "query": {"match_all": {}},
          |  "aggs": {
          |    "groupby_hour": {
          |      "terms": {
          |        "field": "logHour",
          |        "size": 24
          |      }
          |    }
          |  }
          |}
          |""".stripMargin
}
