package com.xunmall.example.message.kafka.producer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

/**
 * @author WangYanjing
 * @description
 * @date 2020/9/17 14:08
 */
public class TransactionConsumeTransformProduce {

    public static final String brokerList = "192.168.79.129:9092";

    public static Properties getConsumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "groupId");
        return properties;
    }

    public static Properties getProducerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionalId");
        return properties;
    }

    public static void main(String[] args) {

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(getConsumerProperties());
        kafkaConsumer.subscribe(Collections.singletonList("topic-source"));

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(getProducerProperties());

        // 初始化事务
        kafkaProducer.initTransactions();

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            if (!records.isEmpty()) {
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(2 << 3);
                // 开启事务
                kafkaProducer.beginTransaction();
                try {
                    for (TopicPartition partition : records.partitions()) {
                        List<ConsumerRecord<String, String>> recordList = records.records(partition);
                        for (ConsumerRecord<String, String> record : recordList) {
                            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("topic-sink", record.key(), record.value());
                            // 消费-生产模型
                            kafkaProducer.send(producerRecord);
                        }
                        Long lastConsumedOffset = recordList.get(recordList.size() - 1).offset();
                        offsets.put(partition, new OffsetAndMetadata(lastConsumedOffset + 1));
                    }
                    // 提交消费位移
                    kafkaProducer.sendOffsetsToTransaction(offsets, "groupId");
                    // 提交事务
                    kafkaProducer.commitTransaction();
                } catch (ProducerFencedException ex) {
                    kafkaProducer.abortTransaction();
                }

            }
        }
    }


}
