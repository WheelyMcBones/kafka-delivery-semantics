package alos;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import java.sql.*;
import java.time.Duration;

public class AtLeastOnceConsumer {

	public static void main(String[] str) throws InterruptedException {

		System.out.println("Starting  AtMostOnceConsumer ...");

		execute();

	}

	private static void execute() throws InterruptedException {

		KafkaConsumer<String, String> consumer = createConsumer();

		// Subscribe to all partition in that topic. 'assign' could be used here
		// instead of 'subscribe' to subscribe to specific partition.
		TopicPartition p0 = new TopicPartition("topic", 0);
		consumer.assign(Arrays.asList(p0));		

		processRecords(consumer);
	}

	private static KafkaConsumer<String, String> createConsumer() {

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		String consumeGroup = "cg1";
		props.put("group.id", consumeGroup);

		// Set this property, if auto commit should happen.
		props.put("enable.auto.commit", "false");

		// This is how to control number of records being read in each poll
		props.put("max.partition.fetch.bytes", "135");

		// Set this if you want to always read from beginning.
		// props.put("auto.offset.reset", "earliest");

		props.put("heartbeat.interval.ms", "3000");
		props.put("session.timeout.ms", "6001");
		props.put("key.deserializer",

				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",

				"org.apache.kafka.common.serialization.StringDeserializer");
		return new KafkaConsumer<String, String>(props);
	}

	private static void processRecords(KafkaConsumer<String, String> consumer) throws InterruptedException {

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
			long lastOffset = 0;
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("\n\roffset = %d, key = %s, value = %s\n", record.offset(), record.key(),
						record.value());
				lastOffset = record.offset();
				process();
			}
			System.out.println("lastOffset read: " + lastOffset);
			consumer.commitSync();
			System.out.println("Committed!");
		}
	}

	private static void process() throws InterruptedException {

		// delay to simulate processing of the message.
		Thread.sleep(500);
	}
}