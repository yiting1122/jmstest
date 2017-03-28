package com.yiting.rabbitmq.pubsub;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Created by hzyiting on 2017/3/27.
 */
public class EmitLog {
	private static final String EXCHANGE_NAME = "logs";
	
	public static void main(String[] args) throws Exception {
		ConnectionFactory connectionFactory = new ConnectionFactory();
		connectionFactory.setHost("10.180.156.42");
		Connection connection = connectionFactory.newConnection();
		Channel channel = connection.createChannel();
		channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
		String msg = "LOG";
		for (int i = 0; i < 100; i++) {
			channel.basicPublish(EXCHANGE_NAME, "", null, (msg + i).getBytes("utf-8"));
			System.out.println(" [x] Sent '" + msg + "'");
		}
		
		channel.close();
		connection.close();
	}
}
