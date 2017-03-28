package com.yiting.rabbitmq;

/**
 * Created by hzyiting on 2017/3/28.
 */
public interface MessageHandler {
	public boolean handle(Message message);
}
