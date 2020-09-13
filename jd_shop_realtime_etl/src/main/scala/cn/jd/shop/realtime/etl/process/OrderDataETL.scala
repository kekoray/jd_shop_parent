package cn.jd.shop.realtime.etl.process

import java.util.Properties

import cn.jd.shop.realtime.etl.`trait`.MysqlBaseETL
import cn.jd.shop.realtime.etl.bean.OrderDBEntity
import cn.jd.shop.realtime.etl.utils.{GlobalConfigUtil, KafkaProps}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.jd.canal_client.bean.CanalRowData
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


/**
 * 导入隐式转换
 */
import org.apache.flink.streaming.api.scala._


/**
 *
 * @ProjectName: jd_shop_parent
 * @program: cn.jd.shop.realtime.etl.process
 * @FileName: OrderDataETL
 * @description: 订单数据的实时ETL操作
 * @version: 1.0
 *           *
 * @author: koray
 * @create: 2020-08-30 20:14
 * @Copyright (c) 2020,All Rights Reserved.
 */
class OrderDataETL(env: StreamExecutionEnvironment) extends MysqlBaseETL(env) {
  /**
   * 定义TEL处理操作的方法,目的是让所有的etl业务实现的时候，都继承自该方法实现其业务逻辑
   */
  override def process(): Unit = {
    //1.接入Kafka数据,过滤出订单数据
    val canalRowDataDataStream: DataStream[CanalRowData] = getKafkaDataStream().filter(_.getTableName == "jd_orders")
    //2.将订单数据转换成订单表对象
    val orderDBEntityDataStream: DataStream[OrderDBEntity] = canalRowDataDataStream.map(OrderDBEntity(_))
    //3.将订单表对象转换为json字符串
    val jsonOrderDBEntityDataStream = orderDBEntityDataStream.map(JSON.toJSONString(_, SerializerFeature.DisableCircularReferenceDetect))
    //测试
    jsonOrderDBEntityDataStream.printToErr("拉宽后的订单数据>>>")
    //4.将数据发送到Kafka中
    jsonOrderDBEntityDataStream.addSink(kafkaProducer(GlobalConfigUtil.`output.topic.order`))

  }


}
