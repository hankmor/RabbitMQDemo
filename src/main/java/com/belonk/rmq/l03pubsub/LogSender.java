package com.belonk.rmq.l03pubsub;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by sun on 2018/3/19.
 *
 * @author sunfuchang03@126.com
 * @version 1.0
 * @since 1.0
 */
public class LogSender {
    /*
     * =================================================================================================================
     *
     * Static fields/constants
     *
     * =================================================================================================================
     */

    static Logger log = LoggerFactory.getLogger(LogSender.class);

    static final String EXCHANGE_NAME = "logs";

    /*
     * =================================================================================================================
     *
     * Instance fields
     *
     * =================================================================================================================
     */



    /*
     * =================================================================================================================
     *
     * Constructors
     *
     * =================================================================================================================
     */



    /*
     * =================================================================================================================
     *
     * Public Methods
     *
     * =================================================================================================================
     */

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        // 创建exchange
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        // 分发消息
        String message = "中文日志信息";
        channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes("utf-8"));
        channel.close();
        connection.close();
    }
    
    /*
     * =================================================================================================================
     *
     * Private Methods
     *
     * =================================================================================================================
     */
     
    /*
     * =================================================================================================================
     *
     * Inner classes
     *
     * =================================================================================================================
     */
}
