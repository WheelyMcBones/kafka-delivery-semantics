package eos.transactions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class TransactionalConsumer {
	private static final String CONSUMER_GROUP_ID = "my-group-id";
	private static final String INPUT_TOPIC = "topic";

	public static void main(String[] str) throws InterruptedException {
		System.out.println("Starting TransactionalConsumer...");
		execute();

	}

	private static void execute() throws InterruptedException {

		KafkaConsumer<String, String> consumer = createConsumer();
		TopicPartition p0 = new TopicPartition(INPUT_TOPIC, 0);
		TopicPartition p1 = new TopicPartition(INPUT_TOPIC, 1);
		TopicPartition p2 = new TopicPartition(INPUT_TOPIC, 2);

//		consumer.subscribe(Collections.singleton(INPUT_TOPIC));

		consumer.assign(Arrays.asList(p0, p1, p2));
		System.out.println("Current offsets p0=" + consumer.position(p0) + " p1=" + consumer.position(p1) + " p2="
				+ consumer.position(p2));

		consumer.seek(p0, getOffsetFromDB(p0));
		consumer.seek(p1, getOffsetFromDB(p1));
		consumer.seek(p2, getOffsetFromDB(p2));
		System.out.println("Fetched Offsets po=" + consumer.position(p0) + " p1=" + consumer.position(p1) + " p2="
				+ consumer.position(p2));

		processRecords(consumer);
	}

	private static KafkaConsumer<String, String> createConsumer() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"); // for Transactional Behaviour
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");

		return new KafkaConsumer<String, String>(props);
	}

	private static void processRecords(KafkaConsumer<String, String> consumer) throws InterruptedException {

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

			long lastOffset = 0;
			boolean printfirstMsg = true;
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("\n\roffset = %d, key = %s, value = %s\n", record.offset(), record.key(),
						record.value());
				lastOffset = record.offset();
				Thread.sleep(1000); // simulating processing
				saveAndCommit(consumer, record);
				System.out.println("Safely saved and committed #offset: " + record.offset() + " - #T: " + record.key());

				// debug
				if (printfirstMsg) {
					System.out.println("First Message - Offset: " + record.offset());
					printfirstMsg = false;
				}

			}
			System.out.println("lastOffset read: " + lastOffset + " - starting processing...");
		}
	}

	private static long getOffsetFromDB(TopicPartition p) {
		long offset = 0;
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test_transactions", "user",
					"userpass");

			String sql = "select offset_num from msg_offsets where topic_name='" + p.topic() + "' and partition_num="
					+ p.partition();
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next())
				offset = rs.getInt("offset_num");
			stmt.close();
			con.close();
		} catch (Exception e) {
			System.out.println("Exception in getOffsetFromDB");
			e.printStackTrace();
		}
		return offset;
	}

	private static void saveAndCommit(KafkaConsumer<String, String> c, ConsumerRecord<String, String> r) {
		System.out.println("Topic=" + r.topic() + " Partition=" + r.partition() + " Offset=" + r.offset() + " Key="
				+ r.key() + " Value=" + r.value());
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test_transactions", "user",
					"userpass");
			con.setAutoCommit(false);

			String insertSQL = "insert into msg_data values(?,?)";
			PreparedStatement psInsert = con.prepareStatement(insertSQL);
			psInsert.setString(1, r.key());
			psInsert.setString(2, r.value());

			String updateSQL = "update msg_offsets set offset_num = offset_num + 1 where topic_name = ? and partition_num = ?";
			PreparedStatement psUpdate = con.prepareStatement(updateSQL);

			psUpdate.setString(1, r.topic());
			psUpdate.setInt(2, r.partition());
			
			c.commitSync();

			psInsert.executeUpdate();
			psUpdate.executeUpdate();
			con.commit();
			con.close();
		} catch (Exception e) {
			System.out.println("Exception in saveAndCommit");
			e.printStackTrace();
		}

	}

}
