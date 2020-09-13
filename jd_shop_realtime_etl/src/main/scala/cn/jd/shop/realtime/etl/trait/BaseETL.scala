package cn.jd.shop.realtime.etl.`trait`

import cn.jd.shop.realtime.etl.utils.KafkaProps
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper

/**
 *
 * @ProjectName: jd_shop_parent  
 * @program: cn.jd.shop.realtime.etl.`trait`
 * @FileName: BaseETL 
 * @descripti :  定义特质，根据数据的来源种类不同，进行封装对应的接口方法
 *            1）定义从Kafka中获取流数据的方法
 *            2）定义TEL处理操作的方法
 *            3）构建kafka的生产者对象
 * @version: 1.0   
 *           *
 * @author: koray   
 * @create: 2020-08-28 23:20  
 * @Copyright (c) 2020,All Rights Reserved.
 */
trait BaseETL[T] {

  /**
   * 定义从Kafka中获取流数据的方法,所有的etl操作都是建立有数据的基础上
   *
   * @param topic
   * @return DataStream
   */
  def getKafkaDataStream(topic: String): DataStream[T]


  /**
   * 定义TEL处理操作的方法,目的是让所有的etl业务实现的时候，都继承自该方法实现其业务逻辑
   */
  def process()


  /**
   * 构建kafka的生产者对象,所有ETL业务处理完之后都写入Kafka中
   *
   * @param topic
   */
  def kafkaProducer(topic: String) = {
    //将所有的ETL处理后的数据写入到kafka集群,写入的时候都是json格式
    new FlinkKafkaProducer011[String](
      topic,
      //这种方式常用于读取的数据来自于kafka，写入的时候也是写到kafka集群
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
      KafkaProps.getProperties())
  }
}
