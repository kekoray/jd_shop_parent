package com.jd.canal_client.kafka;

import com.jd.canal_client.bean.CanalRowData;
import com.jd.canal_client.util.ConfigUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @ProjectName: jd_shop_parent
 * @program: com.jd.canal_client
 * @FileName: KafkaSender
 * @description: kafka生产者工具类
 * @version: 1.0
 * *
 * @author: koray
 * @create: 2020-08-27 20:12
 * @Copyright (c) 2020,All Rights Reserved.
 */
public class KafkaSender {
    //定义properties对象，封装kafka的相关参数
    private Properties kafkaProps = new Properties();
    private KafkaProducer<String, CanalRowData> kafkaProducer;

    /**
     * 构造方法
     */
    public KafkaSender() {
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigUtil.kafkaBootstrap_servers_config());
        kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, ConfigUtil.kafkaBatch_size_config());
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, ConfigUtil.kafkaAcks());
        kafkaProps.put(ProducerConfig.RETRIES_CONFIG, ConfigUtil.kafkaRetries());
        kafkaProps.put(ProducerConfig.CLIENT_ID_CONFIG, ConfigUtil.kafkaClient_id_config());
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ConfigUtil.kafkaKey_serializer_class_config());
        //自定义value的序列化类,接受protobuf的字节数组类型
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ConfigUtil.kafkaValue_serializer_class_config());

        //示例化生产者对象
        kafkaProducer = new KafkaProducer<String, CanalRowData>(kafkaProps);
    }

    /**
     * 将数据写入到kafka集群
     *
     * @param rowData
     */
    public void send(CanalRowData rowData) {
        //ProtoBufSerializer自定义序列化的serialize方法在这里体现
        kafkaProducer.send(new ProducerRecord<String, CanalRowData>(ConfigUtil.kafkaTopic(), rowData));
    }


}
