package cn.jd.shop.realtime.etl.process

import java.util.concurrent.TimeUnit

import cn.jd.shop.realtime.etl.`trait`.MysqlBaseETL
import cn.jd.shop.realtime.etl.async.AsyncGoodsRedisRequest
import cn.jd.shop.realtime.etl.bean.GoodsWideBean
import cn.jd.shop.realtime.etl.utils.GlobalConfigUtil
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
 * @program: cn.jd.shop.realtime.etl
 * @FileName: GoodsDataETL
 * @description: 商品数据的ETL处理
 * @version: 1.0
 *           *
 * @author: koray
 * @create: 2020-08-31 17:15
 * @Copyright (c) 2020,All Rights Reserved.
 */
class GoodsDataETL(env: StreamExecutionEnvironment) extends MysqlBaseETL(env) {

  /**
   * 定义TEL处理操作的方法,目的是让所有的etl业务实现的时候，都继承自该方法实现其业务逻辑
   */
  override def process(): Unit = {
    //1.从Kafka中拉取数据,过滤出商品表数据
    val canalRowDataStream: DataStream[CanalRowData] = getKafkaDataStream().filter(_.getTableName == "jd_goods")

    //2.使用异步io的方式,对商品表数据进行拉宽操作,转换成商品宽表对象返回
    val goodsWideDataStream: DataStream[GoodsWideBean] = AsyncDataStream.unorderedWait(canalRowDataStream,
      new AsyncGoodsRedisRequest(), 100, TimeUnit.SECONDS, 100)

    goodsWideDataStream.printToErr("拉宽后的商品数据>>>")

    //3.把商品宽表对象转换成JSON字符串,发送到Kafka中
    val jsonGoodsWideDataStream: DataStream[String] = goodsWideDataStream.map(JSON.toJSONString(_, SerializerFeature.DisableCircularReferenceDetect))
    jsonGoodsWideDataStream.addSink(kafkaProducer(GlobalConfigUtil.`output.topic.goods`))
  }


}
