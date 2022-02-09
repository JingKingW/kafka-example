package com.xunmall.example.message.simple;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * @Author: WangYanjing
 * @Date: ${Date} ${Time}
 * @Description:
 */
public class Broker {

    private final static int MAX_SIZE = 3;

    private static ArrayBlockingQueue<String> messageQueue = new ArrayBlockingQueue<String>(MAX_SIZE);

    public static void product(String message) {
        if (messageQueue.offer(message)) {
            System.out.println("成功向消息处理中心投递消息：" + message + " ,当前暂存的消息数量是： " + messageQueue.size());
        } else {
            System.out.println("消息处理中心内暂存的消息达到最大的负荷，不能继续放入消息");
        }
        System.out.println("==================================================");
    }

    public static String consume() {
        String message = messageQueue.poll();
        if (message != null) {
            System.out.println("已经消费消息：" + message + " ,当前暂存的数量是： " + messageQueue.size());
        } else {
            System.out.println("消息中心没有可以提供消费的消息！");
        }
        System.out.println("=============================================");
        return message;
    }

}
