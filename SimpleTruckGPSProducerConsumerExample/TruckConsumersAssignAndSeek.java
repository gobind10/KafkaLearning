package com.learning.gobind.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class TruckConsumersAssignAndSeek {
    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "truck_gps_coordinates";
        boolean keepOnreading = true;
        Logger logger = LoggerFactory.getLogger(TruckConsumersAssignAndSeek.class.getName());

        //Create Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Creating Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        int offsetToreadFrom = 0;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //Seek
        consumer.seek(partitionToReadFrom, offsetToreadFrom);

        int numberOfMessagesToRead = 3;
        int numberOfMessagesReadSoFar = 0;

        //Poll for new data
        while(keepOnreading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record: records) {
                logger.info("Truck Number: "+ record.key() + "\n"+ "Truck Co-ordinates:"+record.value()+"\n"+
                        "Topic Partition: "+record.partition() + "\n"+ "Partition Offset:"+record.offset());
            }
        }
    }
}
