package com.atguigu.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

case class StartupLog(mid: String,
                      uid: String,
                      ar: String,
                      ba: String,
                      ch: String,
                      md: String,
                      os: String,
                      vc: String,
                      ts: Long,
                      var logDate: String = null, // 年月日  2020-07-15
                      var logHour: String = null) { //小时  10
  @transient
  //禁止序列化和反序列化
  private val date = new Date(ts)
  logDate = new SimpleDateFormat("yyyy-MM-dd").format(date)
  logHour = new SimpleDateFormat("HH").format(date)
}