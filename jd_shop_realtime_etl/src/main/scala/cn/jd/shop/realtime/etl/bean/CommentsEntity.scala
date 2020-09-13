package cn.jd.shop.realtime.etl.bean

import com.alibaba.fastjson.JSON

import scala.beans.BeanProperty

/**
 *
 * @ProjectName: jd_shop_parent
 * @program: cn.jd.shop.realtime.etl
 * @FileName: CommentsEntity
 * @description: 评论消息表样例类,评论消息宽表样例类
 * @version: 1.0
 *           *
 * @author: koray
 * @create: 2020-08-31 17:57
 * @Copyright (c) 2020,All Rights Reserved.
 */

/**
 * 评论消息表样例类
 *
 * @param userId
 * @param userName
 * @param orderGoodsId
 * @param starScore
 * @param comments
 * @param assetsViedoJSON
 * @param goodsId
 * @param timestamp
 */
case class CommentsEntity(userId: String, // 用户ID
                          userName: String, // 用户名
                          orderGoodsId: String, // 订单明细ID
                          starScore: Int, // 评分
                          comments: String, // 评论
                          assetsViedoJSON: String, // 图片、视频JSO
                          goodsId: String, //商品id
                          timestamp: Long // 评论时间
                         )

/**
 * 评论消息表伴生对象
 */
object CommentsEntity {
  def apply(json: String): CommentsEntity = {
    val jsonObject = JSON.parseObject(json)
    new CommentsEntity(
      jsonObject.getString("userId"),
      jsonObject.getString("userName"),
      jsonObject.getString("orderGoodsId"),
      jsonObject.getInteger("starScore"),
      jsonObject.getString("comments"),
      jsonObject.getString("assetsViedoJSON"),
      jsonObject.getString("goodsId"),
      jsonObject.getLong("timestamp")
    )
  }
}


/**
 * 评论消息宽表样例类
 */
case class CommentsWideEntity(
                               @BeanProperty var userId: String, // 用户ID
                               @BeanProperty var userName: String, // 用户名
                               @BeanProperty var orderGoodsId: String, // 订单明细ID
                               @BeanProperty var starScore: Int, // 评分
                               @BeanProperty var comments: String, // 评论
                               @BeanProperty var assetsViedoJSON: String, // 图片、视频JSO
                               @BeanProperty var createTime: String, // 评论时间
                               @BeanProperty var goodsId: String, // 商品id
                               @BeanProperty var goodsName: String, //商品名称，
                               @BeanProperty var shopId: Long //商家id    ---扩宽后的字段
                             )
