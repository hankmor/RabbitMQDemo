package com.belonk.rmq.l05topic;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
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
public class TopicAnimalInfoSender {
    /*
     * =================================================================================================================
     *
     * Static fields/constants
     *
     * =================================================================================================================
     */

    private static Logger log = LoggerFactory.getLogger(TopicAnimalInfoSender.class);

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
        // 准备数据
        String[] msgs = {
                "quick.orange.rabbit", "lazy.orange.elephant", "lazy.brown.fox", // 能匹配
                "lazy.black.male.cat", // 四个单词也可以匹配
                "orange", "quick.orange.male.rabbit" // 不能匹配，消息被丢弃
        };
        for (String msg : msgs) {
            System.out.println("发送：" + msg);
            channel.basicPublish(exchangeName, msg, null, msg.getBytes("utf-8"));
        }
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
