package cn.jd.shop.realtime.etl.async

import java.sql.{Date, Timestamp}

import cn.jd.shop.realtime.etl.bean.{CommentsEntity, CommentsWideEntity, DimGoodsDBEntity}
import cn.jd.shop.realtime.etl.utils.RedisUtil
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import redis.clients.jedis.Jedis

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

/**
 *
 * @ProjectName: jd_shop_parent  
 * @program: cn.jd.shop.realtime.etl
 * @FileName: AsyncCommentsRedisRequest 
 * @description: 评论表的异步拉宽操作
 * @version: 1.0   
 *           *
 * @author: koray   
 * @create: 2020-08-31 18:06  
 * @Copyright (c) 2020,All Rights Reserved.
 */
class AsyncCommentsRedisRequest() extends RichAsyncFunction[CommentsEntity, CommentsWideEntity] {

  var jedis: Jedis = _

  /**
   * 初始化redis的连接
   *
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {
    jedis = RedisUtil.getJedis()
    //指定redis的数据库
    jedis.select(1)
  }


  /**
   * 定义隐式转换的异步回调的上下文对象
   */
  implicit lazy val executor: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.directExecutor())


  /**
   * 异步处理每条数据
   *
   * @param commentsEntity
   * @param resultFuture
   */
  override def asyncInvoke(commentsEntity: CommentsEntity, resultFuture: ResultFuture[CommentsWideEntity]): Unit = {

    var commentsWideEntity: CommentsWideEntity =  null

    Future {
      if (!jedis.isConnected) {
        jedis = RedisUtil.getJedis()
        jedis.select(1)
      }

      try {

        //获取商品的详细信息
        val goodsJSON = jedis.hget("jd_shop_realtime:jd_goods", commentsEntity.goodsId)
        val dimGoods: DimGoodsDBEntity = DimGoodsDBEntity(goodsJSON)

        //将时间戳转换为时间类型
        val timestamp = new Timestamp(commentsEntity.timestamp)
        val date = new Date(timestamp.getTime)

        commentsWideEntity = CommentsWideEntity(
          commentsEntity.userId,
          commentsEntity.userName,
          commentsEntity.orderGoodsId,
          commentsEntity.starScore,
          commentsEntity.comments,
          commentsEntity.assetsViedoJSON,
          DateFormatUtils.format(date, "yyyy-MM-dd HH:mm:ss"),
          commentsEntity.goodsId,
          dimGoods.goodsName,
          dimGoods.shopId
        )
      } catch {
        case exception: Exception => println(exception)
      }


      resultFuture.complete(Array(commentsWideEntity))

    }
  }


  /**
   * 超时情况会调用的方法, 不重写的话超时会报错...
   *
   * @param input
   * @param resultFuture
   */
  override def timeout(input: CommentsEntity, resultFuture: ResultFuture[CommentsWideEntity]): Unit = {
    println("评论消息宽表拉宽操作的时候，与维度表的数据关联超时了")
  }


  /**
   * 关闭redis连接
   */
  override def close(): Unit = {
    if (jedis.isConnected) {
      jedis.close()
    }
  }
}
