package com.yiting.rabbitmq.singlequeue;

import com.rabbitmq.client.*;

import java.io.IOException;

/**
 * Created by hzyiting on 2017/3/27.
 */
public class Recv {
	private final static String QUEUE_NAME = "HELLO";
	
	public static void main(String[] args) throws Exception {
		ConnectionFactory connectionFactory = new ConnectionFactory();
		connectionFactory.setHost("10.180.156.42");
		Connection connection = connectionFactory.newConnection();
		final Channel channel = connection.createChannel();
		channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
		channel.basicQos(1);
		Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
				System.out.println(consumerTag);
				System.out.println(envelope);
				String message = new String(body, "UTF-8");
				System.out.println(" [x] Received '" + message + "'");
				channel.basicAck(envelope.getDeliveryTag(),false);
			}
		};
		channel.basicConsume(QUEUE_NAME, false, consumer);
	}
}
