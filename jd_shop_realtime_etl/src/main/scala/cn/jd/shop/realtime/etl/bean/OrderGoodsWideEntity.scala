package cn.jd.shop.realtime.etl.bean

import scala.beans.BeanProperty

/**
 *
 * @ProjectName: jd_shop_parent  
 * @program: cn.jd.shop.realtime.etl
 * @FileName: OrderGoodsWideEntity
 * @description: 订单明细宽表样例类
 * @version: 1.0
 *           *
 * @author: koray
 * @create: 2020-08-31 13:01
 * @Copyright (c) 2020,All Rights Reserved.
 */
case class OrderGoodsWideEntity(
                                 @BeanProperty var ogId:Long,                    // 订单明细id
                                 @BeanProperty var orderId:Long,                 // 订单id
                                 @BeanProperty var goodsId:Long,                 // 商品id
                                 @BeanProperty var goodsNum:Long,                // 商品数量
                                 @BeanProperty var goodsPrice:Double,            // 商品价格
                                 @BeanProperty var goodsName:String,             // 商品名称
                                 @BeanProperty var shopId:Long,                  //*拉宽后的字段 --> 商品表id
                                 @BeanProperty var goodsThirdCatId:Int,          //*拉宽后的字段 --> 商品表的商品三级分类id
                                 @BeanProperty var goodsThirdCatName:String,     //*拉宽后的字段 --> 商品分类表的三级分类名称
                                 @BeanProperty var goodsSecondCatId:Int,         //*拉宽后的字段 --> 商品分类表的二级分类id
                                 @BeanProperty var goodsSecondCatName:String,    //*拉宽后的字段 --> 商品分类表的二级分类名称
                                 @BeanProperty var goodsFirstCatId:Int,          //*拉宽后的字段 --> 商品分类表的一级分类id
                                 @BeanProperty var goodsFirstCatName:String,     //*拉宽后的字段 --> 商品分类表的一级分类名称
                                 @BeanProperty var areaId:Int,                   //*拉宽后的字段 --> 店铺表的区域id
                                 @BeanProperty var shopName:String,              //*拉宽后的字段 --> 店铺表的店铺名称
                                 @BeanProperty var shopCompany:String,           //*拉宽后的字段 --> 店铺表的店铺所在公司
                                 @BeanProperty var cityId:Int,                   //*拉宽后的字段 --> 店铺表的城市id
                                 @BeanProperty var cityName:String,              //*拉宽后的字段 --> 组织机构表的城市名称
                                 @BeanProperty var regionId:Int,                 //*拉宽后的字段 --> 组织机构表的大区id
                                 @BeanProperty var regionName:String            //*拉宽后的字段 --> 组织机构表的大区名称
                               )
