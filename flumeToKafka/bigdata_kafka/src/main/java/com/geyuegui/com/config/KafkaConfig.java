package com.geyuegui.com.config;

import com.geyuegui.com.uitl.ConfigUtil;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.util.Properties;

public class KafkaConfig {
    private static final Logger LOG = Logger.getLogger(KafkaConfig.class);
    private static final String DEFAULT_KAFKA_CONFIG_PATH = "/usr/etc/kafka/kafka-server-config.properties";

    private Properties properties;
    private ProducerConfig producerConfig;
    private static volatile KafkaConfig kafkaConfig=null;

    private KafkaConfig(){
        LOG.info("start instance producerConfig");
        properties = ConfigUtil.getInstance().getProperties(DEFAULT_KAFKA_CONFIG_PATH);
//        producerConfig = ProducerConfig.addSerializerToConfig(properties,Serializer<String>,Serializer<String>);
        LOG.info("finish instance producerConfig ");
    }
    public static KafkaConfig getInstance(){
        if(kafkaConfig==null){
            synchronized (KafkaConfig.class){
                if(kafkaConfig==null){
                    kafkaConfig=new KafkaConfig();
                }
            }
        }
        return kafkaConfig;
    }

    public Properties getProperties(){
        return properties;
    }


}
