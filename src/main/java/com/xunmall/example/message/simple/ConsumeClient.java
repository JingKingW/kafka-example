package com.xunmall.example.message.simple;

/**
 * @Author: WangYanjing
 * @Date: ${Date} ${Time}
 * @Description:
 */
public class ConsumeClient {

    public static void main(String[] args) throws Exception {
        String message = MqClient.consume();
        System.out.println(message);
    }

}
