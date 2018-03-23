package com.belonk.rmq.l06rpc;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 斐波那契数列计算服务端。
 * <p>
 * 给定第几个数，计算斐波那契数列中其对应的值。
 * <p>
 * 斐波那契数列：斐波那契数列指的是这样一个数列，这个数列从第3项开始，每一项都等于前两项之和。
 * 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, ...
 * <p>
 * Created by sun on 2018/3/21.
 *
 * @author sunfuchang03@126.com
 * @version 1.0
 * @since 1.0
 */
public class FibonacciRpcServer {
    /*
     * =================================================================================================================
     *
     * Static fields/constants
     *
     * =================================================================================================================
     */

    static String rpcQueueName = "rpc_queue";

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

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.queueDeclare(rpcQueueName, false, false, false, null);
        channel.basicQos(1);

        System.out.println("等待RPC请求");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                // 接收到的请求消息，即传递数列的第n个数
                String msg = new String(body, "utf-8");
                long n = Long.parseLong(msg);
                System.out.println("请求的参数为：" + n);

                long result = fib(n);
                System.out.println("斐波那契数列的第[" + n + "]个数为：" + result);
                String response = String.valueOf(result);

                // 参数properties持有客户端请求设定的属性参数，包括replyTo、correlationId等
                // 将请求的correlationId作为响应的属性，传递给客户端，用于匹配请求和响应
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties().builder()
                        .correlationId(properties.getCorrelationId())
                        .build();

                System.out.println("consumerTag : " + consumerTag);
                System.out.println("envelope.getRoutingKey : " + envelope.getRoutingKey());
                System.out.println("properties.getReplyTo : " + properties.getReplyTo());
                System.out.println("properties.getCorrelationId : " + properties.getCorrelationId());

                channel.basicPublish("", properties.getReplyTo(), replyProps, response.getBytes("utf-8"));
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };

        channel.basicConsume(rpcQueueName, false, consumer);
    }
    
    /*
     * =================================================================================================================
     *
     * Private Methods
     *
     * =================================================================================================================
     */

    private static void testFibonacci() {
        System.out.println(fib(1));
        System.out.println(fib(2));
        System.out.println(fib(3));
        System.out.println(fib(4));
        System.out.println(fib(5));
        System.out.println(fib(6));
    }

    /**
     * 计算斐波那契数列中第n个数的值。
     *
     * @param n 第几个数，从1开始
     * @return 整数值
     */
    private static long fib(long n) {
        if (n == 0) {
            return 0;
        }
        if (n == 1) {
            return 1;
        }
        return fib(n - 1) + fib(n - 2);
    }

    /*
     * =================================================================================================================
     *
     * Inner classes
     *
     * =================================================================================================================
     */
}
