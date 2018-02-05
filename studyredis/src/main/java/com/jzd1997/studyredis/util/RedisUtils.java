package com.jzd1997.studyredis.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jzd1997.studyredis.ApplicationContextProvider;
import com.jzd1997.studyredis.Receiver;

@Component
public class RedisUtils {
	@Autowired
	@Qualifier("redisTemplate")
	RedisTemplate template;

	@Autowired
	ApplicationContextProvider provider;

	private Logger log = LoggerFactory.getLogger(this.getClass());

	@Bean
	RedisMessageListenerContainer container(RedisConnectionFactory connectionFactory,
			MessageListenerAdapter listenerAdapter) {

		RedisMessageListenerContainer container = new RedisMessageListenerContainer();
		container.setConnectionFactory(connectionFactory);
		// container.addMessageListener(listenerAdapter, new PatternTopic("chat"));

		return container;
	}

	@Bean
	MessageListenerAdapter listenerAdapter(Receiver receiver) {
		return new MessageListenerAdapter(receiver, "receiveMessage");
	}

	@Bean
	Receiver receiver(CountDownLatch latch) {
		return new Receiver(latch);
	}

	@Bean
	CountDownLatch latch() {
		return new CountDownLatch(1);
	}

	@Bean
	StringRedisTemplate stringTemplate(RedisConnectionFactory connectionFactory) {
		return new StringRedisTemplate(connectionFactory);
	}

	@PostConstruct
	public void init() {
		// RedisSerializer<String> stringSerializer = new StringRedisSerializer();
		// template.setKeySerializer(stringSerializer);
		// template.setValueSerializer(stringSerializer);
		// template.setHashKeySerializer(stringSerializer);
		// template.setHashValueSerializer(stringSerializer);

		Jackson2JsonRedisSerializer<Object> jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer<Object>(
				Object.class);
		ObjectMapper om = new ObjectMapper();
		om.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
		om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
		jackson2JsonRedisSerializer.setObjectMapper(om);
		template.setKeySerializer(jackson2JsonRedisSerializer);
		template.setValueSerializer(jackson2JsonRedisSerializer);
		template.setHashKeySerializer(jackson2JsonRedisSerializer);
		template.setHashValueSerializer(jackson2JsonRedisSerializer);
		template.afterPropertiesSet();
	}

	public void setValue(String key, Object val) {
		template.opsForValue().set(key, val);
	}

	public void setValue(String key, Object val, int time, TimeUnit unit) {
		template.opsForValue().set(key, val, time, unit);
	}

	public Object getValue(String key) {
		return template.opsForValue().get(key);
	}

	public void multiSet(Map<String, Object> map) {
		template.opsForValue().multiSet(map);
	}

	public List<Object> multiGet(Collection<String> keys) {
		return template.opsForValue().multiGet(keys);
	}

	public long incr(String key, long delta) {
		return template.opsForValue().increment(key, delta);
	}

	public void lpush(String key, String value) {
		template.opsForList().leftPush(key, value);
	}

	public List<Object> range(String key, int start, int end) {
		return template.opsForList().range(key, start, end);
	}

	public Object rpop(String key) {
		return template.opsForList().rightPop(key);
	}

	public void setHash(String key, Map<String, Object> map) {
		template.opsForHash().putAll(key, map);
	}

	public Object getHash(String key, String prop) {
		return template.opsForHash().get(key, prop);
	}

	public Map getHashAll(String key) {
		Map map = new HashMap();
		map.put("keys", template.opsForHash().keys(key));
		map.put("vals", template.opsForHash().values(key));
		return map;
	}

	public void subscribe(String channel) {
		RedisMessageListenerContainer container = provider.getBean(RedisMessageListenerContainer.class);
		MessageListenerAdapter listenerAdapter = provider.getBean(MessageListenerAdapter.class);
		container.addMessageListener(listenerAdapter, new PatternTopic(channel));
	}

	public void publish(String channel, String message) throws InterruptedException {
		CountDownLatch latch = provider.getBean(CountDownLatch.class);

		log.info("Sending message...");
		template.convertAndSend(channel, message);

		latch.await();
	}
}
