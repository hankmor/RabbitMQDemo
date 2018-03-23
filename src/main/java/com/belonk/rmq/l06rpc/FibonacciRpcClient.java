package com.belonk.rmq.l06rpc;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

/**
 * Created by sun on 2018/3/20.
 *
 * @author sunfuchang03@126.com
 * @version 1.0
 * @since 1.0
 */
public class FibonacciRpcClient {
    /*
     * =================================================================================================================
     *
     * Static fields/constants
     *
     * =================================================================================================================
     */

    private static Logger log = LoggerFactory.getLogger(FibonacciRpcClient.class);

    /*
     * =================================================================================================================
     *
     * Instance fields
     *
     * =================================================================================================================
     */

    private Channel channel = null;
    private Connection connection = null;

    /*
     * =================================================================================================================
     *
     * Constructors
     *
     * =================================================================================================================
     */

    public FibonacciRpcClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        this.connection = factory.newConnection();
        this.channel = connection.createChannel();
    }

    /*
     * =================================================================================================================
     *
     * Public Methods
     *
     * =================================================================================================================
     */

    public long call(long n) throws IOException, InterruptedException {
        // 创建默认的回调队列
        String callbackQueueName = channel.queueDeclare().getQueue();
        // 设置消息属性，响应后发送到回调队列中
        final String correlationId = UUID.randomUUID().toString(); // 随机生成唯一标识
        System.out.println("生成的correlationId: " + correlationId);
        AMQP.BasicProperties properties = new AMQP.BasicProperties().builder()
                .correlationId(correlationId) // 每次请求都设定唯一标识，该标识用于将请求和响应进行匹配
                .replyTo(callbackQueueName) // 回调队列
                .build();
        // 发送消息
        String msg = String.valueOf(n); // 消息为斐波那契数列的第几个数
        channel.basicPublish("", "rpc_queue", properties, msg.getBytes("utf-8"));

        // 容量设置为1，在成功获取响应信息之前，其take方法会一直阻塞以等待结果
        final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1);
        // 获取响应
        channel.basicConsume(callbackQueueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("consumerTag : " + consumerTag);
                System.out.println("envelope.getRoutingKey : " + envelope.getRoutingKey());
                System.out.println("properties.getReplyTo : " + properties.getReplyTo());
                String respCorrelationId = properties.getCorrelationId();
                System.out.println("properties.getCorrelationId: " + properties.getCorrelationId());
                // 响应回来的correlationId与生成的匹配，说明是本次请求的响应
                if (correlationId.equals(respCorrelationId)) {
                    response.offer(new String(body, "utf-8"));
                }
            }
        });
        // 阻塞
        System.out.println("计算结果中...");
        return Long.parseLong(response.take());
    }

    public void close() {
        try {
            channel.close();
            connection.close();
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        FibonacciRpcClient client = new FibonacciRpcClient();
        long n = 10;
        long result = client.call(n);
        System.out.println("斐波那契数列的第[" + n + "]个数是：" + result);
        client.close();
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
