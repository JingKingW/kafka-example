package com.xunmall.example.message.kafka.flink;

import java.io.Serializable;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2021/1/12 16:58
 */
public class KafkaRecordDO implements Serializable {
    private String phoneKey;
    private Long time;
    private Long uid;

    public KafkaRecordDO() {
    }

    public KafkaRecordDO(String phoneKey, Long time, Long uid) {
        this.phoneKey = phoneKey;
        this.time = time;
        this.uid = uid;
    }

    public String getPhoneKey() {
        return phoneKey;
    }

    public void setPhoneKey(String phoneKey) {
        this.phoneKey = phoneKey;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Long getUid() {
        return uid;
    }

    public void setUid(Long uid) {
        this.uid = uid;
    }

    @Override
    public String toString() {
        return "KafkaRecordDO{" +
                "phoneKey='" + phoneKey + '\'' +
                ", time=" + time +
                ", uid=" + uid +
                '}';
    }
}
