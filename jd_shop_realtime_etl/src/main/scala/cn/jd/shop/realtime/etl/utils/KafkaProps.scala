package cn.jd.shop.realtime.etl.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

/**
 *
 * @ProjectName: jd_shop_parent  
 * @program: cn.jd.shop.realtime.etl.trait
 * @FileName: KafkaProps 
 * @description: 封装kafka的生产者参数
 * @version: 1.0   
 *           *
 * @author: koray   
 * @create: 2020-08-29 0:00  
 * @Copyright (c) 2020,All Rights Reserved.
 */
object KafkaProps {

  /**
   * 读取Kafka属性配置
   */
  def getProperties(): Properties = {
    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, GlobalConfigUtil.`bootstrap.servers`)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GlobalConfigUtil.`group.id`)
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, GlobalConfigUtil.`enable.auto.commit`)
    properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, GlobalConfigUtil.`auto.commit.interval.ms`)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, GlobalConfigUtil.`auto.offset.reset`)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, GlobalConfigUtil.`key.serializer`)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, GlobalConfigUtil.`key.deserializer`)

    properties
  }

}
