package com.xunmall.example.message.kafka.consumer;

import com.xunmall.example.message.kafka.Company;
import com.xunmall.example.message.kafka.CompanyDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author WangYanjing
 * @date 2020/8/19.
 */
@Slf4j
public class ConsumerFastTest {

    public static final String brokerList = "10.100.31.41:9091";
    public static final String topic = "test-perf";
    public static final String groupId = "gp-localTest";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id" + System.currentTimeMillis());
        return properties;
    }

    public static Properties initDeserializerConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CompanyDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id" + System.currentTimeMillis());
        properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, ConsumerInterceptorTTL.class.getName());
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 60 * 1000);
        return properties;
    }

    /***
     * @Description: [消费者组再平衡]
     * @Title: testConsumerReBL
     * @Author: WangYanjing
     * @Date: 2020/10/29
     * @Param:
     * @Return: void
     * @Throws:
     */
    @Test
    public void testConsumerReBL() {
        Properties properties = initConfig();

        KafkaConsumer consumer = new KafkaConsumer<String, String>(properties);

        Map currentOffsets = new HashMap<TopicPartition, OffsetAndMetadata>(2 << 4);
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                consumer.commitSync(currentOffsets);
                currentOffsets.clear();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {

            }
        });
        try {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                currentOffsets.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), new OffsetAndMetadata(consumerRecord.offset() + 1));
                System.out.println(consumerRecord.value());
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    /**
     * @Description: [测试手动设定消息消费位移值]
     * @Title: testConsumerSeek
     * @Author: WangYanjing
     * @Date: 2020/8/21
     * @Param:
     * @Return: void
     * @Throws:
     */
    @Test
    public void testConsumerSeek() {
        Properties properties = initConfig();

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        kafkaConsumer.subscribe(Arrays.asList(topic));
        // 判定是否为consumer分配相应的分区信息
        Set<TopicPartition> topicPartitionSet = new HashSet<>();
        while (topicPartitionSet.size() == 0) {
            kafkaConsumer.poll(Duration.ofMillis(100));
            topicPartitionSet = kafkaConsumer.assignment();
        }
        // 该方法将所有的分区末尾的消息位置，用于seek方法重新设置；当然也可以通过时间来
        Map<TopicPartition, Long> offsets = kafkaConsumer.endOffsets(topicPartitionSet);

        // 进行分区偏移量进行手动设置
        for (TopicPartition partition : topicPartitionSet) {
            kafkaConsumer.seek(partition, offsets.get(partition));
        }

        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
            for (TopicPartition topicPartition : consumerRecords.partitions()) {
                for (ConsumerRecord<String, String> record : consumerRecords.records(topicPartition)) {
                    System.out.println(record.partition() + " " + record.value());
                }
            }

        }

    }

    /**
     * @Description: [用以判断partition的offset与消费端消费位移及commit offset的关系]
     * @Title: testConsumerOffsetChange
     * @Author: WangYanjing
     * @Date: 2020/8/21
     * @Param:
     * @Return: void
     * @Throws:
     */
    @Test
    public void testConsumerOffsetChange() {
        Properties properties = initConfig();

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        TopicPartition tp = new TopicPartition(topic, 0);

        kafkaConsumer.assign(Arrays.asList(tp));

        long lastConsumerOffset = -1;

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

            if (records.isEmpty()) {
                break;
            }
            List<ConsumerRecord<String, String>> partitionRecords = records.records(tp);

            lastConsumerOffset = partitionRecords.get(partitionRecords.size() - 1).offset();

            kafkaConsumer.commitSync();
        }

        System.out.println("consumer offset is " + lastConsumerOffset);

        OffsetAndMetadata offsetAndMetadata = kafkaConsumer.committed(tp);

        System.out.println("commit offset is " + offsetAndMetadata.offset());

        long position = kafkaConsumer.position(tp);

        System.out.println("the offset of the next record is " + position);

    }

    /**
     * @Description: [自定义反序列化方法的使用]
     * @Title: testConsumerDeserializer
     * @Author: WangYanjing
     * @Date: 2020/8/21
     * @Param:
     * @Return: void
     * @Throws:
     */
    @Test
    public void testConsumerDeserializer() {
        Properties properties = initDeserializerConfig();

        KafkaConsumer<String, Company> kafkaConsumer = new KafkaConsumer<>(properties);

        kafkaConsumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, Company> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
            for (TopicPartition topicPartition : consumerRecords.partitions()) {
                for (ConsumerRecord<String, Company> record : consumerRecords.records(topicPartition)) {
                    System.out.println(record.partition() + " " + record.value());
                }
            }
        }

    }

    /**
     * @Description: [kafka消费端的简单样例]
     * @Title: testConsumerSimple
     * @Author: WangYanjing
     * @Date: 2020/8/21
     * @Param:
     * @Return: void
     * @Throws:
     */
    @Test
    public void testConsumerSimple() {
        Properties properties = initConfig();

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        // 订阅主题消息
        kafkaConsumer.subscribe(Arrays.asList(topic));

        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    System.out.println("topic = " + record.topic() + ", partition = " + record.partition() + " , offset = " + record.offset());
                    System.out.println("key = " + record.key() + " , value = " + record.value());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }
}
