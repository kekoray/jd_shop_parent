package cn.jd.shop.realtime.etl.`trait`

import cn.jd.shop.realtime.etl.utils.{CanalRowDataDeserializationSchema, GlobalConfigUtil, KafkaProps}
import com.jd.canal_client.bean.CanalRowData
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * 手动导入该包
 */
import org.apache.flink.streaming.api.scala._

/**
 *
 * @ProjectName: jd_shop_parent  
 * @program: cn.jd.shop.realtime.etl.`trait`
 * @FileName: MysqlBaseETL 
 * @descripti :  TODO   
 * @version: 1.0   
 *           *
 * @author: koray   
 * @create: 2020-08-29 0:41  
 * @Copyright (c) 2020,All Rights Reserved.
 */
abstract class MysqlBaseETL(env: StreamExecutionEnvironment) extends BaseETL[CanalRowData] {
  /**
   * 定义从Kafka中获取流数据的方法,所有的etl操作都是建立有数据的基础上
   *
   * @param topic
   * @return DataStream
   */
  override def getKafkaDataStream(topic: String = GlobalConfigUtil.`input.topic.canal`): DataStream[CanalRowData] = {
    //消费的是kafka的canal的数据，而binlog日志进行了protobuf的序列化，所以读取到的数据需要反序列化
    val canalKafkaConsumer: FlinkKafkaConsumer011[CanalRowData] = new FlinkKafkaConsumer011[CanalRowData](
      topic,
      new CanalRowDataDeserializationSchema(),
      KafkaProps.getProperties())

    val canalDataStream: DataStream[CanalRowData] = env.addSource(canalKafkaConsumer)

    canalDataStream
  }


}
