package com.atguigu.realtime.bean

case class OrderInfo(id: Long,
                     province_id: Long,
                     order_status: String,
                     user_id: Long,
                     final_total_amount: Double,
                     benefit_reduce_amount: Double,
                     original_total_amount: Double,
                     feight_fee: Double,
                     expire_time: String,
                     create_time: String,
                     operate_time: String,

                     var create_date: String = null, // 年月日
                     var create_hour: String = null, // 小时
                     var is_first_order: Boolean = false, // 是否首单
                     // 来源省份表, 将来和省份表进行join之后, 数据就有了
                     var province_name: String = null,
                     var province_area_code: String = null,
                     var province_iso_code: String = null, //国际地区编码
                     // 来源于用用户表
                     var user_age_group: String = null,
                     var user_gender: String = null) {
    create_date = create_time.substring(0, 10)
    create_hour = create_time.substring(11, 13)
}
