package com.belonk.rmq.l04routing;

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
public class LogSenderDirect {
    /*
     * =================================================================================================================
     *
     * Static fields/constants
     *
     * =================================================================================================================
     */

    static Logger log = LoggerFactory.getLogger(LogSenderDirect.class);

    static final String EXCHANGE_NAME = "direct_logs";

    static Log[] logs = {
            new Log("error", "this is an error log."),
            new Log("error", "this is an error log."),
            new Log("error", "this is an error log."),
            new Log("error", "this is an error log."),
            new Log("warning", "this is a warning log."),
            new Log("info", "this is an info log."),
            new Log("info", "this is an info log."),
            new Log("info", "this is an info log."),
            new Log("debug", "this is a debug log."),
            new Log("debug", "this is a debug log.")
    };

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
        channel.exchangeDeclare(EXCHANGE_NAME, "direct");

        // 分发日志
        for (Log log : logs) {
            System.out.println("log : " + log.getMsg());
            channel.basicPublish(EXCHANGE_NAME, log.getLevel(), null, log.getMsg().getBytes("utf-8"));
        }

        channel.close();
        conn.close();
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

    public static class Log {
        private String level;
        private String msg;

        public Log(String level, String msg) {
            this.level = level;
            this.msg = msg;
        }

        public String getLevel() {
            return level;
        }

        public void setLevel(String level) {
            this.level = level;
        }

        public String getMsg() {
            return msg;
        }

        public void setMsg(String msg) {
            this.msg = msg;
        }
    }
}
