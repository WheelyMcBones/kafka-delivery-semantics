package amos.noretries;

import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

public class AMOSProducer {

	public static void main(String[] str) throws InterruptedException, IOException {

		System.out.println("Starting AMOProducer ...");

		sendMessages();

	}

	private static void sendMessages() throws InterruptedException, IOException {

		Producer<String, String> producer = createProducer();

		sendMessages(producer);

	}

	private static Producer<String, String> createProducer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("retries", 0);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		return new KafkaProducer<String, String>(props);
	}

	private static void sendMessages(Producer<String, String> producer) throws InterruptedException {
		String topic = "topic";
		int partition = 0;
		long record = 1;
		while (true) {
			producer.send(new ProducerRecord<String, String>(topic, partition, Long.toString(record),
					Long.toString(record++)));
			if(record % 1000 == 0) {
				Thread.sleep(2000);
			}

		}
	}

}
