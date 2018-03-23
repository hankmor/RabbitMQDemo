package com.belonk.rmq.l01helloworld;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by sun on 2018/1/17.
 *
 * @author sunfuchang03@126.com
 * @version 1.0
 * @since 1.0
 */
public class Sender {
    //~ Static fields/initializers =====================================================================================

    /**
     * 队列名称
     */
    public static final String QUEUE_NAME = "helloworld";
    //~ Instance fields ================================================================================================


    //~ Constructors ===================================================================================================


    //~ Methods ========================================================================================================
    public static void main(String[] args) throws IOException, TimeoutException {
        // 创建连接工厂
        ConnectionFactory connectionFactory = new ConnectionFactory();
        // 连接到本地server
        connectionFactory.setHost("localhost");
        // 创建连接
        Connection connection = connectionFactory.newConnection();
        // 创建通道，API通过通道完成相关任务
        Channel channel = connection.createChannel();
        // 创建队列，该队列非持久(服务器重启后依然存在)、非独占(非仅用于此链接)、非自动删除(服务器将不再使用的队列删除)
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        String msg = "hello, rabbit mq. 你好，rabbit mq.";
        // 发布消息
        channel.basicPublish("", QUEUE_NAME, null, msg.getBytes("utf-8"));
        System.out.println("Sent \"" + msg + "\".");;
        channel.close();
        connection.close();
    }
}
