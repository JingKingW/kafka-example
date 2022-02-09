package com.xunmall.example.message.simple;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;

/**
 * @Author: WangYanjing
 * @Date: ${Date} ${Time}
 * @Description:
 */
public class MqClient {

    public static void product(String message) throws Exception {
        Socket socket = new Socket(InetAddress.getLocalHost(), BrokerServer.SERVER_PORT);

        try (PrintWriter printWriter = new PrintWriter(socket.getOutputStream())) {
            printWriter.println(message);
            printWriter.flush();
        }
    }

    public static String consume() throws Exception {

        Socket socket = new Socket(InetAddress.getLocalHost(), BrokerServer.SERVER_PORT);
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream())) {
            out.println("CONSUME");
            out.flush();

            String message = bufferedReader.readLine();
            return message;
        }
    }

}
