package com.xunmall.example.message.kafka;

import com.xunmall.example.message.kafka.flink.FastJsonUtils;
import kafka.admin.AdminOperationException;
import kafka.admin.AdminUtils;
import kafka.admin.ConfigCommand;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author WangYanjing
 * @description
 * @date 2020/9/3 9:53
 */
public class KafkaAdminClientTest {

    private AdminClient adminClient;

    @Before
    public void init() {
        String brokerList = "10.100.31.31:9091";
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        adminClient = AdminClient.create(properties);
    }

    @After
    public void destroy() {
        if (adminClient != null) {
            adminClient.close();
        }
    }


    @Test
    public void testCreateTopic() {
        String topic = "topic-abc";
        NewTopic newTopic = new NewTopic(topic, 3, (short) 1);
        CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
        try {
            result.all().get();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        adminClient.close();
    }

    @Test
    public void testDescribeTopicConfig() throws ExecutionException, InterruptedException {
        String topic = "topic-admin";

        ConfigResource resource = new ConfigResource(Type.TOPIC, topic);
        DescribeConfigsResult result = adminClient.describeConfigs(Collections.singleton(resource));
        Config config = result.all().get().get(resource);
        System.out.println(config);
        adminClient.close();
    }

    @Test
    public void testGetGroupsForTopic() throws ExecutionException, InterruptedException {
        String topic = "abc";
        try {
            List<String> allGroups = adminClient.listConsumerGroups()
                    .valid()
                    .get(10, TimeUnit.SECONDS)
                    .stream()
                    .map(ConsumerGroupListing::groupId)
                    .collect(Collectors.toList());
            allGroups.forEach(item -> {
                System.out.println(item);
            });


            Map<String, ConsumerGroupDescription> allGroupDetails = adminClient.describeConsumerGroups(allGroups).all().get(10, TimeUnit.SECONDS);

            allGroupDetails.forEach((key, value) -> {
                System.out.println(key + ": " + value);
            });


            final List<String> filteredGroups = new ArrayList<>();
            allGroupDetails.entrySet().forEach(entry -> {
                String groupId = entry.getKey();
                ConsumerGroupDescription description = entry.getValue();
                boolean topicSubscribed = description.members().stream().map(MemberDescription::assignment)
                        .map(MemberAssignment::topicPartitions)
                        .map(tps -> tps.stream().map(TopicPartition::topic).collect(Collectors.toSet()))
                        .anyMatch(tps -> tps.contains(topic));
                if (topicSubscribed) {
                    filteredGroups.add(groupId);
                }

            });
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void changeTopicByClient() {

        // 方式一
        int DEFAULT_SESSION_TIMEOUT = 30000;
        String zkServer = "10.100.31.31:2181/kafka";
        String topicName = "test-perf";
        Properties topicConfig = new Properties();
        topicConfig.setProperty("retention.ms", 6 * 60 * 1000 + "");
        ZkUtils zkUtils = ZkUtils.apply(zkServer, DEFAULT_SESSION_TIMEOUT, DEFAULT_SESSION_TIMEOUT, JaasUtils.isZkSecurityEnabled());
        AdminUtils.changeTopicConfig(zkUtils, topicName, topicConfig);

        // 方式二
        ConfigResource configResource = new ConfigResource(Type.TOPIC, topicName);
        ConfigEntry entry = new ConfigEntry("retention.ms", 6 * 60 * 1000 + "");
        Config config = new Config(Collections.singletonList(entry));
        Map<ConfigResource, Config> configMap = new HashMap<>();
        configMap.put(configResource, config);
        try {
            adminClient.alterConfigs(configMap).all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void changeTopicByCommand() {
        String[] agrs = {"--zookeeper", "10.100.31.31:2181/kafka", "--alter", "--entity-type", "topics", "--entity-name", "test-perf", "--add-config", "retention.ms=6400000"};
        ConfigCommand.main(agrs);
    }

    @Test
    public void changeTopicByApi() throws InterruptedException {
        String topic = "test-perf";
        Properties configs = new Properties();
        configs.put("retention.ms", "3200000");
        // 判断对应主题是否存在
        ZkClient zkClient = new ZkClient("10.100.31.41:2181/kafka", 30000, 30000, new BytesPushThroughSerializer());
        if (!topicExists(zkClient, topic)) {
            throw new AdminOperationException("Topic " + topic + " does not exist.");
        }
        // 将变更配置写入/config/topics/主题中
        // 格式{"version":1,"config":{"retention.ms":"6400000"}}
        String topicPath = "/config/topics/" + topic;
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("version", 1);
        configMap.put("config", new HashMap<>(configs));
        String content = FastJsonUtils.convertObjectToJSON(configMap);
        System.out.println(content);
        zkClient.writeData(topicPath, content.getBytes());
        Thread.sleep(1000);
        byte[] readData = zkClient.readData(topicPath, true);
        System.out.println(new String(readData));

        // 将变更通知写入/config/changes/config_change_0000000017
        // 格式{"version":1,"entity_type":"topics","entity_name":"test-perf"}
        String configPath = "/config/changes/config_change_";
        Map<String, Object> changeMap = new HashMap<>();
        changeMap.put("version", 1);
        changeMap.put("entity_type", "topics");
        changeMap.put("entity_name", topic);
        String value = FastJsonUtils.convertObjectToJSON(changeMap);
        System.out.println(value);
        zkClient.create(configPath, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

        zkClient.close();
    }

    private static Boolean topicExists(ZkClient zkUtils, String topic) {
        return zkUtils.exists(getTopicPath(topic));
    }

    private static String getTopicPath(String topic) {
        return "/brokers/topics" + "/" + topic;
    }

}
