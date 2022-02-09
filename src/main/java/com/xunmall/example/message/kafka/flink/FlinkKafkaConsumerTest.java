package com.xunmall.example.message.kafka.flink;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2021/1/13 10:35
 */
public class FlinkKafkaConsumerTest {


    public static final String brokerList = "10.100.31.31:9091";
    public static final String groupId = "rtc-dhContactGP";

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return properties;
    }

    @Test
    public void testConsumerSimple() {
        Properties properties = initConfig();

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        List<String> topicList = new ArrayList<>();
        topicList.add("dhContactUpload");
/*        topicList.add("dhDailyActive");
        topicList.add("dhUserInited");
        topicList.add("dhUserInfoChange");*/
        topicList.add("kkmonitor_alarm");

        // 订阅主题消息
        kafkaConsumer.subscribe(topicList);

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + " , offset = " + record.offset());
                }
                Thread.sleep(10* 1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }

}
