package com.geyuegui.com.producer;

import com.geyuegui.com.config.KafkaConfig;
import kafka.producer.KeyedMessage;
import org.apache.log4j.Logger;
import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class LogProducer {
    private static final Logger LOG = Logger.getLogger(LogProducer.class);

    //send data method
    public static void producer(String topic, String line){
        Properties props = KafkaConfig.getInstance().getProperties();
        Producer<String, String> producer = new KafkaProducer(props);
        ProducerRecord<String, String> stringProducerRecord = new ProducerRecord(topic, line);
        producer.send(stringProducerRecord);
        LOG.info("to kafka"+topic+"send data success:"+line);
        LOG.info("to kafka"+topic+"send data complete");
        producer.flush();
//        producer.close();
    }

    public static void main(String[] args) {
        String line = "66666666";
        LogProducer.producer("test1",line);
    }

}
