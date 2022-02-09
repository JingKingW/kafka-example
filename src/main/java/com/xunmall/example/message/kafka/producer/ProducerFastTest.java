package com.xunmall.example.message.kafka.producer;

import com.xunmall.example.message.kafka.Company;
import com.xunmall.example.message.kafka.CompanySerializer;
import com.xunmall.example.message.kafka.SimplePartitioner;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author wangyanjing
 * @date 2020/8/19
 */
public class ProducerFastTest {

    public static final String brokerList = "10.100.31.31:9091";
    public static final String topic = "test-perf";

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        return properties;
    }


    public static Properties initSerializerConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.client.id.demo");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorPrefix.class.getName());
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, SimplePartitioner.class.getName());
        return properties;
    }

    @Test
    public void testProducerTTL() throws ExecutionException, InterruptedException {
        Properties properties = initConfig();

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        ProducerRecord<String, String> record1 = new ProducerRecord<>(topic, 0, System.currentTimeMillis() - 10 * 10000, null, "first-expire-data");
        producer.send(record1).get();

        ProducerRecord<String, String> record2 = new ProducerRecord<>(topic, 0, System.currentTimeMillis(), null, "second-expire-data");
        producer.send(record2).get();

        ProducerRecord<String, String> record3 = new ProducerRecord<>(topic, 0, System.currentTimeMillis() - 10 * 10000, null, "three-expire-data");
        producer.send(record3).get();
    }


    /**
     * 实践基础kafka生产端功能
     */
    @Test
    public void testSimpleStart() {
        Properties properties = initConfig();

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, "Hello World!");

        // 同步发送消息
        Future<RecordMetadata> future = producer.send(producerRecord);

        try {
            RecordMetadata recordMetadata = future.get();
            System.out.println(recordMetadata.topic() + "-" + recordMetadata.partition() + ":" + recordMetadata.offset());

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        producer.close();
    }

    /**
     * @Description: [执行异步发送多条消息]
     * @Title: testProducerSendAsync
     * @Author: WangYanjing
     * @Date: 2020/8/25
     * @Param:
     * @Return: void
     * @Throws:
     */
    @Test
    public void testProducerSendAsync() throws InterruptedException {
        Properties properties = initConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        ProducerRecord record = null;
        // 异步发送多条消息
        int i = 0;
        while (i < 10000) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(i++);
            stringBuilder.append(",wangyanjing,abc123");
            String key = "abc@" + System.currentTimeMillis();
            record = new ProducerRecord(topic, key, stringBuilder.toString());
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

    }


    /**
     * @Description: [实践自定义序列化发送消息]
     * @Title: testProducerSerializer
     * @Author: WangYanjing
     * @Date: 2020/8/20
     * @Param:
     * @Return: void
     * @Throws:
     */
    @Test
    public void testProducerSerializer() throws ExecutionException, InterruptedException {
        Properties properties = initSerializerConfig();

        KafkaProducer<String, Company> producer = new KafkaProducer<>(properties);

        Company company = Company.builder().name("WangYanjing").address("Shanghai").build();

        ProducerRecord<String, Company> record = new ProducerRecord<String, Company>(topic, company);

        producer.send(record).get();

    }
}
