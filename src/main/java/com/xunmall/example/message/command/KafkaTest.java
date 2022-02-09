package com.xunmall.example.message.command;

import kafka.admin.AdminUtils;
import kafka.admin.ConfigCommand;
import kafka.admin.ConsumerGroupCommand;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.Test;

import java.util.Properties;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2021/5/24 19:50
 */
public class KafkaTest {

    @Test
    public void changeTopic() {
        int DEFAULT_SESSION_TIMEOUT = 30000;
        String zkServer = "10.100.31.31:2181/kafka";
        String topicName = "test";
        Properties topicConfig = new Properties();
        topicConfig.setProperty("retention.ms", 10 * 60 * 1000 + "");

        ZkUtils zkUtils = ZkUtils.apply(zkServer, DEFAULT_SESSION_TIMEOUT, DEFAULT_SESSION_TIMEOUT, JaasUtils.isZkSecurityEnabled());
        AdminUtils.changeTopicConfig(zkUtils, topicName, topicConfig);

    }

    @Test
    public void changeTopicByCommand(){
        String[] agrs = {"--zookeeper", "10.100.31.31:2181/kafka", "--alter", "--entity-type","topics", "--entity-name", "test-perf", "--add-config", "retention.ms=86400000"};
        ConfigCommand.main(agrs);
    }
}
