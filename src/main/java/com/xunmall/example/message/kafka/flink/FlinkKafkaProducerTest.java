package com.xunmall.example.message.kafka.flink;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;
import java.util.Random;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2021/1/12 17:04
 */
public class FlinkKafkaProducerTest {

    public static final String brokerList = "10.100.31.31:9091";

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");
        return properties;
    }

    @Test
    public void testProducerSendAsync() throws InterruptedException {
        Properties properties = initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        ProducerRecord record = null;
        String topic = "dhComputingHighNodePhone";
        // 异步发送多条消息
        int i = 0;
        while (i < 10) {
            KafkaRecordDO kafkaRecordDO = new KafkaRecordDO();
            kafkaRecordDO.setPhoneKey("3fcaf1ffc5cc8aeebaeb21c5475ed619");
            kafkaRecordDO.setUid(5321445850940416L);
            kafkaRecordDO.setTime(System.currentTimeMillis());
            String messageRecord = FastJsonUtils.convertObjectToJSON(kafkaRecordDO);
            record = new ProducerRecord(topic, messageRecord);
            Thread.sleep(new Random().nextInt(100));
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        // 增加正常的容错记录信息
                        e.printStackTrace();
                    } else {
                        System.out.println("topic: " + recordMetadata.topic() + "-- partition:" + recordMetadata.partition() + "--offset:" + recordMetadata.offset());
                    }
                }
            });
            i++;
        }
        producer.close();

    }


    @Test
    public void testProducerSendUser() throws InterruptedException {
        Properties properties = initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        ProducerRecord record = null;
        String topic = "bizType-user";
        String key = "user.inited";
        // 异步发送多条消息
        int i = 0;
        while (i < 1000000) {
            UserInfoDO userInfoDO = new UserInfoDO();

            String messageRecord = FastJsonUtils.convertObjectToJSON(userInfoDO);
            record = new ProducerRecord(topic, key, messageRecord);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        // 增加正常的容错记录信息
                        e.printStackTrace();
                    } else {
                        System.out.println("topic: " + recordMetadata.topic() + "-- partition:" + recordMetadata.partition() + "--offset:" + recordMetadata.offset());
                    }
                }
            });
            i++;
        }
        producer.close();
    }


}
