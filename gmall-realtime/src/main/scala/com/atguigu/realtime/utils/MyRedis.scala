package com.atguigu.realtime.utils

import redis.clients.jedis.Jedis

case object MyRedis {
  def getClient() = {
    new Jedis("hadoop162",6379)
  }

}
