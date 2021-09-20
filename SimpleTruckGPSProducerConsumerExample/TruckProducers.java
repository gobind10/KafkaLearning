package com.learning.gobind.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;

public class TruckProducers {
    public static void main(String[] args) {
        String topic = "truck_gps_coordinates";
        String bootstrapServer = "127.0.0.1:9092";
        Logger logger = LoggerFactory.getLogger(TruckProducers.class.getName());

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Creating Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Creating ProducerRecord
        String[] truck_keys = {"PB02AQ1015", "PB02HG1018", "PB02SS1045"};
        for (String key: truck_keys) {
            for(int i = 1; i<=5; i++){
                String value = new String(Math.random() * 100 +"," + Math.random()* 100);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                //Sending data
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e == null) {
                            logger.info("Truck Number: "+key + "\n" +
                                    "Truck Co-ordinates: "+ value + "\n" +
                                    "Topic Name: "+ recordMetadata.topic() + "\n" +
                                    "Partition: "+ recordMetadata.partition() + "\n" +
                                    "Offset: "+ recordMetadata.offset() + "\n" +
                                    "Timestamp: "+ recordMetadata.timestamp());
                        }
                        else {
                            logger.error("Problem in producer");
                        }
                    }
                });
            }
        }
        producer.flush();
        producer.close();
    }
}
