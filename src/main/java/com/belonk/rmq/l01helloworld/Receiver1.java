package com.belonk.rmq.l01helloworld;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by sun on 2018/1/17.
 *
 * @author sunfuchang03@126.com
 * @version 1.0
 * @since 1.0
 */
public class Receiver1 {
    //~ Static fields/initializers =====================================================================================


    //~ Instance fields ================================================================================================


    //~ Constructors ===================================================================================================


    //~ Methods ========================================================================================================

    public static void main(String[] args) throws IOException, TimeoutException {
        // 创建连接工厂
        ConnectionFactory connectionFactory = new ConnectionFactory();
        // 连接到远端server
        connectionFactory.setHost("192.168.0.25");
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("123456");
        // 创建连接
        Connection connection = connectionFactory.newConnection();
        // 创建通道，API通过通道完成相关任务
        Channel channel = connection.createChannel();
        // 创建队列
        channel.queueDeclare(Sender.QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        // 创建消费者，阻塞接收消息
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("consumerTag : " + consumerTag);
                System.out.println("exchange : " + envelope.getExchange());
                System.out.println("routing key : " + envelope.getRoutingKey());
                String msg = new String(body, "utf-8");
                System.out.println("Received \"" + msg + "\".");
            }
        };
        channel.basicConsume(Sender.QUEUE_NAME, consumer);
//        channel.close();
//        connection.close();
    }
}
