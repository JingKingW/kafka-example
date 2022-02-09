package com.xunmall.example.message.kafka.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author WangYanjing
 * @description
 * @date 2020/8/20 11:20
 */
public class ProducerInterceptorPrefix implements ProducerInterceptor<String, String> {
    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        String modifiedValue = "prefix-" + producerRecord.value();
        return new ProducerRecord<String, String>(producerRecord.topic(), producerRecord.partition(), producerRecord.timestamp(), producerRecord.key(), modifiedValue, producerRecord.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e == null){
            sendSuccess ++;
        }else{
            sendFailure ++;
        }
    }

    @Override
    public void close() {
        double successRatio = (double) sendSuccess / (sendSuccess + sendFailure);
        System.out.println("[INFO] 发送成功率 =" + String.format("%f",successRatio * 100) + "%");
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}