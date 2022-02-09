package com.xunmall.example.message.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;
import java.util.Random;

/**
 * @author WangYanjing
 * @description
 * @date 2020/8/27 10:30
 */
public class MonitorProducer {

    public static final String brokerList = "192.168.79.129:9092";
    public static final String topic = "topic-admin";

    public Properties initConfig(String host) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "monitor-client-test");
        return properties;
    }

    @Test
    public void testSendGateWayMenu() {
        Properties properties = initConfig("192.168.79.129:9091");

        KafkaProducer producer = new KafkaProducer<>(properties);

        String msg = "{\"resKey\":\"one-gateway-monitor-admin-monitor.abcTestabc.list\",\"dimensions\":\"max,fail,avg,total,tpslimit,ratelimit,fuse,isolate\",\"topMenu\":\"one-gateway\",\"menuLevels\": [\n" +
                "        {\n" +
                "            \"key\": \"one-gateway\",\n" +
                "            \"name\":\"网关服务集\"\n" +
                "        },        {\n" +
                "            \"key\": \"monitor-admin\",\n" +
                "            \"name\":\"监控管理后台\"\n" +
                "        },        {\n" +
                "            \"key\": \"monitor.abcTestabc.list\",\n" +
                "            \"name\":\"abcTestabc\"\n" +
                "        }\n" +
                "    ]}";

        ProducerRecord producerRecord = new ProducerRecord<String, String>("bizType-monitor-test", System.currentTimeMillis() + "", msg);

        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null) {
                    e.printStackTrace();
                } else {
                    System.out.println(recordMetadata.toString());
                }
            }
        });

        producer.close();
    }

    @Test
    public void testBatchSenderMsg() throws InterruptedException {
        Properties properties = initConfig(brokerList);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        ProducerRecord record = null;
        // 异步发送多条消息
        for (int i = 0; i < 1000000; i++) {
            record = new ProducerRecord(topic, "msg" + i);
            Thread.sleep(new Random().nextInt(100));
            producer.send(record);
        }
        producer.close();
    }


}
