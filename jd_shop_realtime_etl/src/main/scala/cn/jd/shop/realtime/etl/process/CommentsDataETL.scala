package cn.jd.shop.realtime.etl.process

import java.util.concurrent.TimeUnit

import cn.jd.shop.realtime.etl.`trait`.MQBaseETL
import cn.jd.shop.realtime.etl.async.AsyncCommentsRedisRequest
import cn.jd.shop.realtime.etl.bean.{CommentsEntity, CommentsWideEntity}
import cn.jd.shop.realtime.etl.utils.GlobalConfigUtil
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}


/**
 * 隐式转换
 */
import org.apache.flink.streaming.api.scala._


/**
 *
 * @ProjectName: jd_shop_parent
 * @program: cn.jd.shop.realtime.etl
 * @FileName: CommentsDataETL
 * @description: 评论数据的实时ETL
 * @version: 1.0
 *           *
 * @author: koray
 * @create: 2020-08-31 17:53
 * @Copyright (c) 2020,All Rights Reserved.
 */
class CommentsDataETL(env: StreamExecutionEnvironment) extends MQBaseETL(env) {

  /**
   * 定义TEL处理操作的方法,目的是让所有的etl业务实现的时候，都继承自该方法实现其业务逻辑
   */
  override def process(): Unit = {
    //1.从Kafka中拉取评论消息数据
    val commentsDS: DataStream[String] = getKafkaDataStream(GlobalConfigUtil.`input.topic.comments`)

    //2.将评论消息数据转换成评论消息表对象
    val commentsEntityDS: DataStream[CommentsEntity] = commentsDS.map(CommentsEntity(_))

    //3.使用异步io的方式,对评论消息表进行拉宽操作,转换成评论消息宽表对象返回
    val commentsWideEntityDS: DataStream[CommentsWideEntity] = AsyncDataStream.unorderedWait(commentsEntityDS,
      new AsyncCommentsRedisRequest(), 100, TimeUnit.SECONDS, 100)

    commentsWideEntityDS.printToErr("拉宽后评论信息>>>")

    //4.将评论消息宽表对象转换成JSON字符串,发送到Kafka中
    val jsonCommentsWideEntityDS: DataStream[String] = commentsWideEntityDS.map(JSON.toJSONString(_, SerializerFeature.DisableCircularReferenceDetect))
    jsonCommentsWideEntityDS.addSink(kafkaProducer(GlobalConfigUtil.`output.topic.comments`))
  }
}
