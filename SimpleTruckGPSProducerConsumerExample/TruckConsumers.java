package com.learning.gobind.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TruckConsumers {
    public static void main(String[] args) throws InterruptedException {
        String bootstrapServer = "127.0.0.1:9092";
        String group_id = "Truck_Consumer_Group";
        String topic = "truck_gps_coordinates";
        boolean keepOnreading = true;
        Logger logger = LoggerFactory.getLogger(TruckConsumers.class.getName());

        //Create Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Creating Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Subscribing consumer to topic
        consumer.subscribe(Arrays.asList(topic));

        //Poll for new data
        while(keepOnreading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5)); // It contains list of partition records for each topic
            for (ConsumerRecord<String, String> record: records) {
                logger.info("Truck Number: "+ record.key() + "\n"+ "Truck Co-ordinates:"+record.value()+"\n"+
                        "Topic Partition: "+record.partition() + "\n"+ "Partition Offset:"+record.offset());
                TimeUnit.SECONDS.sleep(1);
            }
        }
    }
}
