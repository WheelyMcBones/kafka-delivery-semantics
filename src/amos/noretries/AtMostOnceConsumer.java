package amos.noretries;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import java.sql.*;
import java.time.Duration;

public class AtMostOnceConsumer {

	public static void main(String[] str) throws InterruptedException {

		System.out.println("Starting  AtMostOnceConsumer ...");

		execute();

	}

	private static void execute() throws InterruptedException {

		KafkaConsumer<String, String> consumer = createConsumer();

		TopicPartition p0 = new TopicPartition("topic", 0);
		consumer.assign(Arrays.asList(p0));

		processRecords(consumer);
	}

	private static KafkaConsumer<String, String> createConsumer() {

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		String consumeGroup = "cg1";
		props.put("group.id", consumeGroup);

		// Set this property to true, if auto commit should happen.
		props.put("enable.auto.commit", "false");

		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return new KafkaConsumer<String, String>(props);
	}

	private static void processRecords(KafkaConsumer<String, String> consumer) throws InterruptedException {

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
			consumer.commitAsync(); // --> early committing: message loss in case of failures
			
			long lastOffset = 0;
			boolean printfirstMsg = true;
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("\n\roffset = %d, key = %s, value = %s\n", record.offset(), record.key(),
						record.value());
				lastOffset = record.offset();
				
				// debug
				if (printfirstMsg) {
					System.out.println("First Message - Offset: " + record.offset());
					printfirstMsg = false;
				}
				
			}
			System.out.println("lastOffset read: " + lastOffset);
			
		}
	}

	private static void process() throws InterruptedException {

		// create some delay to simulate processing of the message.
		Thread.sleep(2200);
	}
}