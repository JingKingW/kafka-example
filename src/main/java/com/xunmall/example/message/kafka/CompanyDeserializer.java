package com.xunmall.example.message.kafka;


import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author WangYanjing
 * @description
 * @date 2020/8/20 14:47
 */
public class CompanyDeserializer implements Deserializer<Company> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Company deserialize(String topic, byte[] data) {
        if (data == null){
            return null;
        }
        if (data.length < 0){
            throw new SerializationException("Size of data received " + "by CompanyDeserializer is shorter than expected!");
        }
        ByteBuffer byteBuffer  = ByteBuffer.wrap(data);
        int nameLen, addreddLen;
        String name,address;

        nameLen = byteBuffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        byteBuffer.get(nameBytes);
        addreddLen = byteBuffer.getInt();
        byte[] addressBytes = new byte[addreddLen];
        byteBuffer.get(addressBytes);

        try {
            name = new String(nameBytes,"UTF-8");
            address = new String(addressBytes,"UTF-8");
        }catch (Exception ex){
            throw new SerializationException(" Error occur when deserializer!");
        }
        return new Company(name,address);
    }

    @Override
    public void close() {

    }
}
