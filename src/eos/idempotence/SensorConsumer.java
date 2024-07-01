package eos.idempotence;

import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.*;
import java.sql.*;
import java.time.Duration;

public class SensorConsumer {

	public static void main(String[] args) throws Exception {

		String topicName = "SensorTopic";
		KafkaConsumer<String, String> consumer = null;
		int rCount;

		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

		consumer = new KafkaConsumer<>(props);
		TopicPartition p0 = new TopicPartition(topicName, 0);
		TopicPartition p1 = new TopicPartition(topicName, 1);
		TopicPartition p2 = new TopicPartition(topicName, 2);

		consumer.assign(Arrays.asList(p0, p1, p2));
		System.out.println("Current position p0=" + consumer.position(p0) + " p1=" + consumer.position(p1) + " p2="
				+ consumer.position(p2));

		consumer.seek(p0, getOffsetFromDB(p0));
		consumer.seek(p1, getOffsetFromDB(p1));
		consumer.seek(p2, getOffsetFromDB(p2));
		System.out.println("New positions po=" + consumer.position(p0) + " p1=" + consumer.position(p1) + " p2="
				+ consumer.position(p2));

		try {
			do {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
				System.out.println("Record polled: " + records.count());
				rCount = records.count();
				for (ConsumerRecord<String, String> record : records) {
					saveAndCommit(consumer, record);
				}
			} while (rCount > 0);
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			consumer.close();
		}
	}

	private static long getOffsetFromDB(TopicPartition p) {
		long offset = 0;
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "user", "userpass");

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
			Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "user", "userpass");
			con.setAutoCommit(false);

			String insertSQL = "insert into msg_data values(?,?)";
			PreparedStatement psInsert = con.prepareStatement(insertSQL);
			psInsert.setString(1, r.key());
			psInsert.setString(2, r.value());

			String updateSQL = "update msg_offsets set offset_num = offset_num + 1 where topic_name = ? and partition_num = ?";
			PreparedStatement psUpdate = con.prepareStatement(updateSQL);

			psUpdate.setString(1, r.topic());
			psUpdate.setInt(2, r.partition());
<<<<<<< HEAD
			
			c.commitSync();
=======
>>>>>>> 5eb41b176b1d4d1e520c472ecdb2715df2cf70c0

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
