package cn.jd.shop.realtime.etl.`trait`

import cn.jd.shop.realtime.etl.utils.KafkaProps
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011


/**
 * 手动导入该包
 */
import org.apache.flink.streaming.api.scala._

/**
 *
 * @ProjectName: jd_shop_parent
 * @program: cn.jd.shop.realtime.etl.`trait`
 * @FileName: MQBaseETL
 * @descripti :  对于来自点击流日志,购物车,评论的数据，因为写入到kafka的数据是字符串类型的数据，因此直接消费出来即可
 * @version: 1.0
 *           *
 * @author: koray
 * @create: 2020-08-29 0:21
 * @Copyright (c) 2020,All Rights Reserved.
 */
abstract class MQBaseETL(env: StreamExecutionEnvironment) extends BaseETL[String] {
  /**
   * 定义从Kafka中获取流数据的方法,所有的etl操作都是建立有数据的基础上
   *
   * @param topic
   * @return DataStream
   */
  override def getKafkaDataStream(topic: String): DataStream[String] = {
    //创建消费者对象，从kafka中消费数据，消费到的数据是字符串类型
    val kafkaConsumer: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String](
      topic,
      new SimpleStringSchema(),
      KafkaProps.getProperties()
    )

    //加载Kafka的数据源
    val logDataStream: DataStream[String] = env.addSource(kafkaConsumer)
    logDataStream
  }
}
