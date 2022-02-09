package com.xunmall.example.message.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author WangYanjing
 * @description
 * @date 2020/8/20 11:07
 */
public class SimplePartitioner implements Partitioner {

    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(s);
        int numberPartitions = partitionInfoList.size();
        if (null == bytes){
            return  counter.getAndIncrement() % numberPartitions;
        }else {
            return Utils.toPositive(Utils.murmur2(bytes)) % numberPartitions;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
