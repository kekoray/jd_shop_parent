package com.jd.canal_client.util;

import java.util.Properties;

/**
 * @ProjectName: jd_shop_parent
 * @program: com.jd.canal_client.util
 * @FileName: ConfigUtil
 * @description: 读取配置文件信息
 * @version: 1.0
 * *
 * @author: koray
 * @create: 2020-08-27 19:46
 * @Copyright (c) 2020,All Rights Reserved.
 */
public class ConfigUtil {
    //定义properties对象
    private static Properties properties;

    static {
        try {
            properties = new Properties();
            //读取配置文件
            properties.load(ConfigUtil.class.getClassLoader()
                    .getResourceAsStream("config.properties"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String canalserverip() {
        return properties.getProperty("canal.server.ip");
    }

    public static int canalserverport() {
        return Integer.parseInt(properties.getProperty("canal.server.port"));
    }

    public static String canalserverdestination() {
        return properties.getProperty("canal.server.destination");
    }

    public static String canalserverusername() {
        return properties.getProperty("canal.server.username");
    }

    public static String canalserverpassword() {
        return properties.getProperty("canal.server.password");
    }

    public static String canalSubscribeFilter() {
        return properties.getProperty("canal.subscribe.filter");
    }

    public static String zookeeperServerIp() {
        return properties.getProperty("zookeeper.server.ip");
    }

    public static String kafkaBootstrap_servers_config() {
        return properties.getProperty("kafka.bootstrap_servers_config");
    }

    public static String kafkaBatch_size_config() {
        return properties.getProperty("kafka.batch_size_config");
    }

    public static String kafkaAcks() {
        return properties.getProperty("kafka.acks");
    }

    public static String kafkaRetries() {
        return properties.getProperty("kafka.retries");
    }

    public static String kafkaBatch() {
        return properties.getProperty("kafka.batch");
    }

    public static String kafkaClient_id_config() {
        return properties.getProperty("kafka.client_id_config");
    }

    public static String kafkaKey_serializer_class_config() {
        return properties.getProperty("kafka.key_serializer_class_config");
    }

    public static String kafkaValue_serializer_class_config() {
        return properties.getProperty("kafka.value_serializer_class_config");
    }

    public static String kafkaTopic() {
        return properties.getProperty("kafka.topic");
    }

    /**
     * 测试
     * @param args
     */
    public static void main(String[] args) {
        //System.out.println(canalserverip());
    }
}
