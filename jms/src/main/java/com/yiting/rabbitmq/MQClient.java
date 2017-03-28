package com.yiting.rabbitmq;

/**
 * Created by hzyiting on 2017/3/28.
 */
/**
 * (C) Copyright Netease.com, Inc. 2015. All Rights Reserved.
 */

import com.rabbitmq.client.*;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

/**
 *
 */
public class MQClient {
	private static Logger logger = Logger.getLogger(MQClient.class);
	
	private static int DEFAULT_PORT = 5672;
	
	private static int DEFAULT_WAIT_TIMEOUT = 30000;
	
	private static String DEFAULT_EXCHANGE = "x";
	
	private static int DEFAULT_CONNECTION_TIMEOUT = 5000; // socket连接超时，单位ms。
	
	private static int DEFAULT_HEARTBEAT = 60; // AMQP心跳，单位s。
	
	private String hostName = null;
	
	private static MQClient client;
	
	private volatile Connection connection = null;
	
	private Map<String, Channel> chanMap = new ConcurrentHashMap<String, Channel>();
	private Map<String, Boolean> chanClosedMap = new ConcurrentHashMap<String, Boolean>(); // 是否显式要求关闭。
	
	private List<String> brokeHosts = new ArrayList<String>();
	
	private int brokePort;
	
	private String userName;
	
	private String password;
	
	private String service;
	
	private volatile int iter = 0;
	
	private MQClient() {
	}
	
	public static MQClient getInstance() {
		synchronized (MQClient.class) {
			if (client == null) {
				client = new MQClient();
			}
		}
		return client;
	}
	
	private synchronized Connection obtainConnection() {
		if (connection != null) {
			return connection;
		}
		chanMap.clear();
		chanClosedMap.clear();
		ConnectionFactory factory = new ConnectionFactory();
		while (true) {
			try {
				factory.setHost(brokeHosts.get(iter % brokeHosts.size()));
				iter++;
				factory.setPort(brokePort);
				factory.setUsername(userName);
				factory.setPassword(password);
				factory.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
				factory.setRequestedHeartbeat(DEFAULT_HEARTBEAT);
				connection = factory.newConnection();
				connection.addShutdownListener(new ShutdownListener() {
					@Override
					public void shutdownCompleted(ShutdownSignalException cause) {
						logger.info("shutdown exception.", cause);
						if (cause.isHardError()) {
							// connection error.
							Connection conn = (Connection) cause.getReference();
							synchronized (client) {
								if (conn == connection) {
									logger.info("try to close connection from shutdown listener.");
									closeConn();
								} else {
									logger.info("connection maybe already closed.");
								}
							}
						}
					}
				});
				logger.info("new connection created: " + connection);
				return connection;
			} catch (Exception e) {
				logger.warn("create connection error! ", e);
				closeConn();
				sleep(3);
			}
		}
	}
	
	private synchronized Channel obtainChannel(String tag) {
		if (chanClosedMap.get(tag) != null) {
			if (chanClosedMap.get(tag).booleanValue()) {
				return null; // 显式关闭channel，不进行重连尝试。
			}
		}
		while (true) {
			try {
				connection = obtainConnection();
				if (chanMap.get(tag) != null) {
					return chanMap.get(tag);
				}
				Channel c = connection.createChannel();
				logger.info("new channel[" + tag + "] created: " + c + " in connection: " + connection);
				if (MQConst.TAG_PUBLISH.equalsIgnoreCase(tag)) {
					c.confirmSelect();
				}
				chanMap.put(tag, c);
				return c;
			} catch (Exception e) {
				logger.warn("create channel error!", e);
				closeConn();
				sleep(3);
			}
		}
	}
	
	public synchronized void init(String brokeHost, int brokePort, String userName, String password, String service) {
		logger.info("broke list : " + brokeHost);
		String[] brokes = brokeHost.split(",");
		for (String broke : brokes) {
			this.brokeHosts.add(broke);
		}
		this.brokePort = brokePort;
		this.userName = userName;
		this.password = password;
		this.service = service;
		initBasicInfras(); // 基础队列组件创建。
	}
	
	/**
	 * to=service表示发送给服务的任意一个节点。 to=service.hostname表示发送给服务的指定节点。
	 *
	 * @param to
	 * @param msg
	 * @return
	 */
	public boolean sendMessage(String to, String body) {
		return sendMessage(to, body, false);
	}
	
	public boolean sendMessageToAll(String to, String body) {
		return sendMessage(to, body, true);
	}
	
	private boolean sendMessage(String to, String body, boolean toAll) {
		String tag = MQConst.TAG_PUBLISH;
		Connection conn = null;
		try {
			Channel c = obtainChannel(tag);
			if (c == null) {
				logger.info("channel is closed manual.");
				return false;
			}
			conn = c.getConnection();
			String from = this.service + "." + getHostname();
			Message msg = new Message(to, from, body);
			String m = JSON.toJSONString(msg);
			String routerKey = to;
			if (toAll) {
				routerKey = to + ".#";
			}
			c.basicPublish(DEFAULT_EXCHANGE, routerKey, null, m.getBytes());
			c.waitForConfirms(DEFAULT_WAIT_TIMEOUT);
			logger.info("send message succ:" + m);
			return true;
		} catch (AlreadyClosedException ace) {
			logger.warn("send message error.", ace);
			if (ace.isHardError()) { // connection error.
				Connection closedConn = (Connection) ace.getReference();
				synchronized (client) {
					if (closedConn == connection) {
						logger.info("try to close connection from send message exception.");
						closeConn();
					}
				}
			} else { // channel error.
				removeCachedChannel(tag);
			}
		} catch (Exception e) {
			logger.warn("send message error!", e);
			synchronized (client) {
				if (conn == connection) {
					logger.info("try to close connection from send message exception.");
					closeConn();
				}
			}
		}
		return false;
	}
	
	public void consumeMessage(MessageHandler handler) {
		String tag = MQConst.TAG_CONSUME;
		Connection conn = null;
		try {
			Channel c = obtainChannel(tag);
			if (c == null) {
				logger.info("channel is closed manual.");
				return;
			}
			conn = c.getConnection();
			LinkedBlockingQueue<QueueingConsumer.Delivery> blockingQueue = new LinkedBlockingQueue<QueueingConsumer.Delivery>();
			QueueingConsumer consumer = new QueueingConsumer(c, blockingQueue);
			if (!conn.isOpen() || !c.isOpen()) {
				throw new Exception("connection broken.");
			}
			c.basicConsume(this.service, false, "consume1", consumer);
			c.basicConsume(this.service + "." + getHostname(), false, "consume2", consumer);
			
			while (true) {
				QueueingConsumer.Delivery delivery = consumer.nextDelivery(DEFAULT_WAIT_TIMEOUT);
				logger.debug("consume delivery result: " + (delivery == null ? "wait timeout" : "normal"));
				if (delivery != null) {
					long deliveryTag = delivery.getEnvelope().getDeliveryTag();
					String m = new String(delivery.getBody());
					try {
						Message msg = JSON.parseObject(m, Message.class);
						boolean result = handler.handle(msg);
						logger.info("consume message:" + m + ", handle resule:" + result);
						if (result) {
							c.basicAck(deliveryTag, false);
						} else {
							c.basicNack(deliveryTag, false, true);
						}
					} catch (Exception e) {
						logger.warn("msg handle error. " + m);
						c.basicAck(deliveryTag, false);
						continue;
					}
					
				}
			}
		} catch (AlreadyClosedException ace) {
			logger.warn("consume message error.", ace);
			if (ace.isHardError()) { // connection error.
				Connection closedConn = (Connection) ace.getReference();
				synchronized (client) {
					if (closedConn == connection) {
						logger.info("try to close connection from consume message exception.");
						closeConn();
					}
				}
			} else { // channel error.
				removeCachedChannel(tag);
			}
		} catch (ShutdownSignalException sse) { // 主动关闭channel
			logger.warn("consume message error.", sse);
			if (sse.isHardError()) { // connection error.
				Connection closedConn = (Connection) sse.getReference();
				synchronized (client) {
					if (closedConn == connection) {
						logger.info("try to close connection from consume message exception.");
						closeConn();
					}
				}
			} else { // channel error.
				removeCachedChannel(tag);
			}
		} catch (Exception e) {
			logger.warn("consume message error!", e);
			synchronized (client) {
				if (conn == connection) {
					logger.info("try to close connection from consume message exception.");
					closeConn();
				}
			}
		}
		consumeMessage(handler);
	}
	
	/**
	 * close channel explicit。
	 *
	 * @param tag
	 */
	public synchronized void closeChannel(String tag) {
		if (chanMap.get(tag) != null) {
			Channel c = chanMap.get(tag);
			try {
				c.close();
			} catch (Exception ignore) {
			}
			chanClosedMap.put(tag, true);
			logger.info("channel closed manual. tag = " + tag + ", channel = " + c);
		}
	}
	
	private void initBasicInfras() {
		try {
			// init exchange
			Channel c = obtainChannel(MQConst.TAG_PUBLISH);
			if (c == null) {
				logger.info("channel is closed manual.");
				return;
			}
			c.exchangeDeclare(DEFAULT_EXCHANGE, "topic", true);
			
			// init queues & binding
			c.queueDeclare(this.service, true, false, false, null);
			c.queueBind(this.service, DEFAULT_EXCHANGE, this.service);
			
			String hostname = getHostname();
			c.queueDeclare(this.service + "." + hostname, true, false, false, null);
			c.queueBind(this.service + "." + hostname, DEFAULT_EXCHANGE, this.service + "." + hostname);
		} catch (IOException e) {
			logger.warn("basic infras inited error. ", e);
		}
	}
	
	private String getHostname() {
		if (StringUtils.isBlank(hostName)) { // 不必在意此处的线程安全问题。
			try {
				hostName = InetAddress.getLocalHost().getHostName();
			} catch (Exception e) {
				logger.debug("cat not get local hostname, try to read /etc/hostname");
				try {
					hostName = FileUtils.readFileToString(new File("/etc/hostname"));
				} catch (Exception error) {
					logger.error("Can not get local hostname.", error);
				}
			}
			if (StringUtils.isBlank(hostName)) {
				hostName = "unknown";
			}
			hostName = hostName.trim(); // 过滤可能存在的回车.
		}
		return hostName;
	}
	
	/**
	 * in second
	 *
	 * @param s
	 */
	private void sleep(long s) {
		try {
			Thread.sleep(s * 1000L);
		} catch (InterruptedException ignore) {
		}
	}
	
	private synchronized void closeConn() {
		if (connection != null) {
			try {
				logger.info("try to close specific connection : " + connection);
				chanMap.clear();
				chanClosedMap.clear();
				connection.close();
				logger.info("specific connection closed.");
			} catch (Exception ignore) {
			}
			connection = null;
		}
	}
	
	private synchronized void removeCachedChannel(String tag) {
		chanMap.remove(tag);
	}
	
	public static void main(String[] args) throws Exception {
		final MQClient client = MQClient.getInstance();
		client.init("10.180.148.10", 5672, "admin", "test", "LOCAL");
		new Thread(new Runnable() {
			@Override
			public void run() {
				client.consumeMessage(new MessageHandler() {
					public boolean handle(Message message) {
						return true;
					}
				});
			}
		}).start();
		// client.closeChannel(MQConst.TAG_CONSUME);
		client.sendMessage("LOCAL", "haha");
	}
}

