package eos.transactions;

import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;

public class TransactionalProducer {
	private static final String TRANSACTION_ID = "prod-1";
	private static final String OUTPUT_TOPIC = "topic";
	
	public static void main(String[] str) throws InterruptedException, IOException {

		System.out.println("Starting TransactionalProducer ...");

		sendMessages();

	}

	private static void sendMessages() throws InterruptedException, IOException {

		Producer<String, String> producer = createProducer();
		producer.initTransactions(); // Phase 1): register to transaction coordinator 

		sendMessages(producer);

	}

	private static Producer<String, String> createProducer() {
		Properties props = new Properties();

		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // for Idempotent Operation
		props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTION_ID); // for transactional behaviour
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");

		return new KafkaProducer<String, String>(props);
	}

	private static void sendMessages(Producer<String, String> producer) throws InterruptedException {
		int partition = 0, numTrans = 0;

		try {

			while (true) {

				// ---- Begin of Transaction ----
				producer.beginTransaction();
				long record = 0, count = 0;
				for (int i = 0; i < 100; i++) {
					if (i % 5 == 0) {
						System.out.println("Currently at: " + record + "/5050 - #Transaction: " + numTrans);
						producer.send(new ProducerRecord<String, String>(OUTPUT_TOPIC, partition, Long.toString(numTrans),
								Long.toString(record))); // Phase 3): sending messages part of the transaction
						
						count++; 
						if (count % 30 == 0) partition++;
					}
					
					record += i;
				}
				
				System.out.println("Sum ended: now simulating processing...");
				Thread.sleep(7000);

				producer.commitTransaction(); // Phase 4) Two-Phase Commit to all 
				// ---- Transaction Committed ----
				numTrans++;
			}

		} catch (KafkaException e) {

			producer.abortTransaction();
			e.printStackTrace();
			// ---- Transaction Aborted ----

		}
	}
}
