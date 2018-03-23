package com.belonk.rmq.l04routing;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import static com.belonk.rmq.l04routing.LogSenderDirect.logs;

/**
 * Created by sun on 2018/3/20.
 *
 * @author sunfuchang03@126.com
 * @version 1.0
 * @since 1.0
 */
public class LogReceiverDirect {
    /*
     * =================================================================================================================
     *
     * Static fields/constants
     *
     * =================================================================================================================
     */

    private static Logger log = LoggerFactory.getLogger(LogReceiverDirect.class);

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
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();

        // 创建direct交换器
        channel.exchangeDeclare(LogSenderDirect.EXCHANGE_NAME, "direct");
        // 创建随机队列
        String queueName = channel.queueDeclare().getQueue();
        // 绑定队列
        // System.out.println("routingKey: " + "error");
        // channel.queueBind(queueName, LogSenderDirect.EXCHANGE_NAME, "error");
        System.out.println("routingKey: " + "debug");
        channel.queueBind(queueName, LogSenderDirect.EXCHANGE_NAME, "debug");
        System.out.println("routingKey: " + "info");
        channel.queueBind(queueName, LogSenderDirect.EXCHANGE_NAME, "info");
        System.out.println("routingKey: " + "warning");
        channel.queueBind(queueName, LogSenderDirect.EXCHANGE_NAME, "warning");

        // 处理日志
        channel.basicConsume(queueName, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("log : " + new String(body, "utf-8"));
            }
        });
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
