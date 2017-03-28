package com.yiting.rabbitmq.pubsub;

import com.rabbitmq.client.*;

import java.io.IOException;

/**
 * Created by hzyiting on 2017/3/27.
 */
public class ConcusmerLog {
	private static final String EXCHANGE_NAME = "logs";
	
	public static void main(String[] args) throws Exception {
		ConnectionFactory connectionFactory = new ConnectionFactory();
		connectionFactory.setHost("10.180.156.42");
		Connection connection = connectionFactory.newConnection();
		Channel channel = connection.createChannel();
		channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
		String queueName = channel.queueDeclare("q2", false, false, false, null).getQueue();
		channel.queueBind(queueName,EXCHANGE_NAME,"");
		Consumer consumer=new DefaultConsumer(channel){
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
				String message = new String(body, "UTF-8");
				System.out.println(" [x] Received '" + message + "'");
			}
		};
		channel.basicConsume(queueName, true, consumer);
		
	}
	
}
