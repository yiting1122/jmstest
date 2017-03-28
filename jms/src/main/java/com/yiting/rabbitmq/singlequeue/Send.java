package com.yiting.rabbitmq.singlequeue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;

/**
 * Created by hzyiting on 2017/3/27.
 */
public class Send {
	private final static String QUEUE_NAME = "HELLO";
	
	public static void main(String[] args) throws Exception {
		ConnectionFactory connectionFactory = new ConnectionFactory();
		connectionFactory.setHost("10.180.156.42");
		Connection connection = connectionFactory.newConnection();
		Channel channel = connection.createChannel();
		channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		String msg = "hello world!";
		for (int i = 0; i < 100; i++) {
			msg = msg + i;
			channel.basicPublish("", QUEUE_NAME, null, msg.getBytes("utf-8"));
			System.out.println(" [x] Sent '" + msg + "'");
		}
		
		channel.close();
		connection.close();
	}
	
}
