package com.gk.kafka;

import java.util.Properties;

import com.alibaba.fastjson.JSONObject;
import com.gk.domain.Student;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MyProducer {
	
	public static void main(String[] args) {
		Properties properties = new Properties();
//		properties.put("bootstrap.servers", "192.168.0.197:9092,192.168.0.198:9092,192.168.0.199:9092");
		properties.put("bootstrap.servers", "192.168.31.251:9092,192.168.31.252:9092,192.168.31.253:9092");
		properties.put("acks", "all");
		properties.put("retries", 0);
		properties.put("batch.size", 16384);
		properties.put("linger.ms", 1);
		properties.put("buffer.memory", 33554432);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		Producer<String, String> producer = new KafkaProducer<>(properties);
		for(int i = 1; i <= 100; i++) {
			Student student = new Student(i, "name" + i, i);
			producer.send(new ProducerRecord<String, String>("student", "key_" + i, JSONObject.toJSONString(student)));
		}
		producer.close();
	}
}
