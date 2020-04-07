package com.geyuegui.com.uitl;


import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigUtil {
    private static final Logger LOG = Logger.getLogger(ConfigUtil.class);
    private static volatile ConfigUtil configUtil;
    private ConfigUtil(){};
    public static ConfigUtil getInstance(){
        if(configUtil==null){
            synchronized (ConfigUtil.class){
                if(configUtil==null){
                    configUtil=new ConfigUtil();
                }
            }
        }
        return configUtil;
    }
    public Properties getProperties(String path) {
        Properties properties = new Properties();
        try {
            LOG.info("path:"+path);
            LOG.info("========starting load propertis");
            LOG.info("========ConfigUtil.getClass():"+this.getClass());
            LOG.info("========ConfigUtil.getClass().getClassLoader():"+this.getClass().getClassLoader());
            File file=new File(path);
            InputStream inputStream=new FileInputStream(file);
//            InputStream inputStream =this.getClass().getClassLoader().getResourceAsStream(path);
            properties.load(inputStream);
            LOG.info("========loaded  propertis successed");
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("========loaded  propertis fail");
        }
//        properties.setProperty("bootstrap.servers","hadoop-5:9092");
//        properties.setProperty("zookeeper.connect","hadoop-4:2181,hadoop-5:2181,hadoop-6:2181");
//        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
//        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
//        properties.setProperty("request.timeout.ms","60000");
//        properties.setProperty("producer.type","sync");
        return properties;
    }

    public static void main(String[] args) {
        String path = "kafka/kafka-server-config.properties";
        Properties properties = ConfigUtil.getInstance().getProperties(path);
        properties.keySet().forEach(key->{
            System.out.println(key);
        });
    }
}
