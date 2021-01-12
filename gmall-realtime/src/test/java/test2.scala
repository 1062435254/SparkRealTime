import java.time.LocalDate

import redis.clients.jedis.Jedis

object test2 {
  def main(args: Array[String]): Unit = {
    println(LocalDate.now().toString)

  }
}
