package com.belonk.rmq.workqueue;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 持久化工作消费者。
 * <p>
 * Created by sun on 2018/1/24.
 *
 * @author sunfuchang03@126.com
 * @version 1.0
 * @since 1.0
 */
public class DurableWorker {
    //~ Static fields/initializers =====================================================================================


    //~ Instance fields ================================================================================================


    //~ Constructors ===================================================================================================


    //~ Methods ========================================================================================================

    /**
     * Worker进程。
     * <p>
     * 任务发布者每发送一个"."号，线程睡眠1秒，模拟工作时间。
     * <p>
     * 可以看到各个Worker轮询执行任务。
     *
     * @param args 参数
     * @throws IOException
     * @throws TimeoutException
     */
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();
        // 持久化设置，true时，队列在RabbitMQ重启或者挂掉时不会丢失
        boolean durable = true;
        channel.queueDeclare(DurableNewTask.TASK_QUEUE_NAME, durable, false, false, null);
        // 创建消费者，阻塞接收消息
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String msg = new String(body, "utf-8");
                System.out.println("Received \"" + msg + "\".");
                try {
                    doWork(msg);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    System.out.println(" [x] Done");
                    // 处理完成，手动接收消息时，需要在处理成功后进行反馈，保证消息不丢失
                    // 如果消费者工作进程挂掉，自动分给其他消费者
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };
        // 自动接收消息并处理，接收后，立即将消息标记为删除，如果工作进程挂掉，者其未处理和处理中的消息都将丢失
//        boolean autoAck = true; // acknowledgment is covered below
        // 手动接收消息，处理完成后调用channel.basicAck()反馈给RabbitMQ
        boolean autoAck = false;
        channel.basicConsume(DurableNewTask.TASK_QUEUE_NAME, autoAck, consumer);
    }

    private static void doWork(String task) throws InterruptedException {
        System.out.println("Doing some works.");
        for (char ch : task.toCharArray()) {
            // 模拟工作时间消耗
            if (ch == '.') {
                Thread.sleep(1000);
            }
        }
    }
}
