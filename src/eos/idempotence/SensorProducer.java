package eos.idempotence;

import java.util.*;
import org.apache.kafka.clients.producer.*;

public class SensorProducer {

	public static void main(String[] args) throws Exception {

		String topicName = "SensorTopic";

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
<<<<<<< HEAD
		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "eos.idempotence.SensorPartitioner");
=======
		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "eos.ext.SensorPartitioner");
>>>>>>> 5eb41b176b1d4d1e520c472ecdb2715df2cf70c0
		props.put("speed.sensor.name", "TSS");
		props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		props.put(ProducerConfig.ACKS_CONFIG, "all");


		Producer<String, String> producer = new KafkaProducer<>(props);

		for (int i = 0; i < 10; i++)
			producer.send(new ProducerRecord<>(topicName, "SSP" + i, "500" + i));

		for (int i = 0; i < 1000; i++)
			producer.send(new ProducerRecord<>(topicName, "TSS", "500" + i));

		producer.close();

		System.out.println("SensorProducer ends.");
	}
}
