package com.gk.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class MyConsumer {

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		Properties properties = new Properties();
//		properties.put("bootstrap.servers", "192.168.0.197:9092,192.168.0.198:9092,192.168.0.199:9092");
		properties.put("bootstrap.servers", "192.168.31.251:9092,192.168.31.252:9092,192.168.31.253:9092");
		//每个消费者分配独立的组号
		properties.put("group.id", "test_group");
		//如果value合法，则自动提交偏移量
		properties.put("enable.auto.commit", "true");
		//设置多久一次更新被消费消息的偏移量
		properties.put("auto.commit.interval.ms", "1000");
		//设置会话响应的时间，超过这个时间kafka可以选择放弃消费或者消费下一条消息
		properties.put("session.timeout.ms", "30000");
		//自动重置offset
		properties.put("auto.offset.reset", "earliest");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(Arrays.asList("student"));
		while (true) {
			//方法已过期
//			consumer.poll(100);
			ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("offset = %d, key = %s, value = %s",record.offset(), record.key(), record.value());
                System.out.println();
			}
		}
	}

}
