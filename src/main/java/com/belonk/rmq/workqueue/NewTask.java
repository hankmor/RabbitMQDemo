package com.belonk.rmq.workqueue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 任务发布者。
 * <p>
 * Created by sun on 2018/1/18.
 *
 * @author sunfuchang03@126.com
 * @version 1.0
 * @since 1.0
 */
public class NewTask {
    //~ Static fields/initializers =====================================================================================

    public static final String TASK_QUEUE_NAME = "hello";
    //~ Instance fields ================================================================================================


    //~ Constructors ===================================================================================================


    //~ Methods ========================================================================================================

    /**
     * 发布5条消息，每条消息的一个“.”表示消息执行时间需要1秒。
     *
     * @param args
     * @throws IOException
     * @throws TimeoutException
     */
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        String[] msgs = new String[]{
                "First Message.", // 1s
                "Second Message..", // 2s
                "Third Message...", // 3s
                "Fourth Message....", // 4s
                "Fifth Message....." // 5s
        };

        Channel channel = connection.createChannel();
        channel.queueDeclare(TASK_QUEUE_NAME, false, false, false, null);

        for (String msg : msgs) {
            // 发布消息
            channel.basicPublish("", TASK_QUEUE_NAME, null, msg.getBytes("utf-8"));
            System.out.println("[x] Sent '" + msg + "'");
        }
        channel.close();
        connection.close();
    }
}
