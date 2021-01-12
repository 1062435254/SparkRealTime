package com.atguigu.gmalllogger.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@Slf4j
public class LoggerController {
    @PostMapping("/apploy")
    public String doLog(@RequestBody String body){
        System.out.println(body);

        //数据落盘，给离线使用
        saveDisk(body);

        //数据写入kafka
        sendToKafka(body);
        return "ok";
    }

    @Autowired
    KafkaTemplate<String,String> kafka;
    //写输入到kafka
    private void sendToKafka(String body) {
        JSONObject json = JSON.parseObject(body);
        if (json.containsKey("start") && json.getString("start").length()>10) {
            kafka.send("gmall_start_topic",body);
        }
        else if(!json.containsKey("start")){
            kafka.send("gmall_event_topic",body);
        }

    }

    //写输入到磁盘
    private void saveDisk(String body) {
        log.info(body);
    }
}
