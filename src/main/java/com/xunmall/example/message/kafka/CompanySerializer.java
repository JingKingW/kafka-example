package com.xunmall.example.message.kafka;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author WangYanjing
 * @date 2020/8/19.
 */
public class CompanySerializer implements Serializer<Company> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public byte[] serialize(String s, Company company) {
        if (company == null) {
            return null;
        }
        byte[] name, address;
        try {
            if (company.getName() != null) {
                name = company.getName().getBytes("UTF-8");
            } else {
                name = new byte[0];
            }
            if (company.getAddress() != null) {
                address = company.getAddress().getBytes("UTF-8");
            } else {
                address = new byte[0];
            }

            ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + name.length + address.length);
            byteBuffer.putInt(name.length);
            byteBuffer.put(name);
            byteBuffer.putInt(address.length);
            byteBuffer.put(address);
            return byteBuffer.array();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
