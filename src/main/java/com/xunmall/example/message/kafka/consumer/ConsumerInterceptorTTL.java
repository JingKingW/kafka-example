package com.xunmall.example.message.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author WangYanjing
 * @description
 * @date 2020/8/25 18:29
 */
public class ConsumerInterceptorTTL implements ConsumerInterceptor<String,String> {

    private static final long EXPIRE_INTERVAL = 10 * 1000;

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> consumerRecords) {
        long now = System.currentTimeMillis();
        Map<TopicPartition,List<ConsumerRecord<String,String>>> newRecords = new HashMap<>(2<<4);
        //  首先获取所有的分区信息
        for (TopicPartition topicPartition : consumerRecords.partitions()){
            // 再获取消费的分区及对应分区中的消息
            List<ConsumerRecord<String,String>> tpRecords = consumerRecords.records(topicPartition);
            List<ConsumerRecord<String,String>> newTpRecords = new ArrayList<>();
            //  最后将消息体进行循环判定是否需要过滤
            for (ConsumerRecord<String,String> record : tpRecords){
                if (now - record.timestamp() < EXPIRE_INTERVAL){
                    newTpRecords.add(record);
                }
            }
            if (!newTpRecords.isEmpty()){
                newRecords.put(topicPartition,newTpRecords);
            }
        }

        return new ConsumerRecords<>(newRecords);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> map) {
        map.forEach((tp,offset)->{
            System.out.println(tp + ":" + offset.offset());
        });
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
