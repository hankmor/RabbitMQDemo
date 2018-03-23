package com.belonk.rmq.l05topic;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by sun on 2018/3/20.
 *
 * @author sunfuchang03@126.com
 * @version 1.0
 * @since 1.0
 */
public class TopicAnimalInfoReceiver {
    /*
     * =================================================================================================================
     *
     * Static fields/constants
     *
     * =================================================================================================================
     */

    private static Logger log = LoggerFactory.getLogger(TopicAnimalInfoReceiver.class);

    static String exchangeName = "animals";

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

        // 创建交换器
        channel.exchangeDeclare(exchangeName, "topic");
        // 创建队列
        String queueName = channel.queueDeclare().getQueue();
        System.out.println("queue : " + queueName);
        // 绑定队列
        // Q1
        // String bindingKey = "*.orange.*";
        // channel.queueBind(queueName, exchangeName, bindingKey);
        // Q2
        String bindingKey = "*.*.rabbit";
        channel.queueBind(queueName, exchangeName, bindingKey);
        bindingKey = "lazy.#";
        channel.queueBind(queueName, exchangeName, bindingKey);
        // 接收消息
        channel.basicConsume(queueName, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String msg = new String(body, "utf-8");
                System.out.println("收到消息：" + msg);
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
