package cn.jd.shop.realtime.etl.bean

import java.text.SimpleDateFormat
import java.util.{Locale}

import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.time.{DateFormatUtils, FastDateFormat}
import scala.beans.BeanProperty

/**
 *
 * @ProjectName: jd_shop_parent  
 * @program: cn.jd.shop.realtime.etl.process
 * @FileName: CaetBean
 * @description: 购物车表样例类 , 购物车宽表样例类
 * @version: 1.0
 *           *
 * @author: koray
 * @create: 2020-08-31 15:25
 * @Copyright (c) 2020,All Rights Reserved.
 */

/**
 * 购物车表样例类
 *
 * @param goodsId
 * @param userId
 * @param count
 * @param guid
 * @param addTime
 * @param ip
 */
case class CartBean(goodsId: String,
                    userId: String,
                    count: Integer,
                    guid: String,
                    addTime: Long,
                    ip: String)

/**
 * 购物车表伴生对象
 */
object CartBean {
  def apply(json: String): CartBean = {
    val jsonObject = JSON.parseObject(json)
    CartBean(
      jsonObject.getString("goodsId"),
      jsonObject.getString("userId"),
      jsonObject.getInteger("count"),
      jsonObject.getString("guid"),
      jsonObject.getLong("addTime"),
      jsonObject.getString("ip")
    )
  }
}


/**
 * 购物车宽表样例类
 *
 * @param goodsId
 * @param userId
 * @param count
 * @param guid
 * @param addTime
 * @param ip
 * @param goodsPrice
 * @param goodsName
 * @param goodsCat3
 * @param goodsCat2
 * @param goodsCat1
 * @param shopId
 * @param shopName
 * @param shopProvinceId
 * @param shopProvinceName
 * @param shopCityId
 * @param shopCityName
 * @param clientProvince
 * @param clientCity
 */
case class CartWideBean(
                         @BeanProperty goodsId: String, //商品id
                         @BeanProperty userId: String, //用户id
                         @BeanProperty count: Integer, //商品数量
                         @BeanProperty guid: String, //用户唯一标识
                         @BeanProperty addTime: String, //添加购物车时间
                         @BeanProperty ip: String, //ip地址
                         @BeanProperty var goodsPrice: Double, //商品价格
                         @BeanProperty var goodsName: String, //商品名称
                         @BeanProperty var goodsCat3: String, //商品三级分类
                         @BeanProperty var goodsCat2: String, //商品二级分类
                         @BeanProperty var goodsCat1: String, //商品一级分类
                         @BeanProperty var shopId: String, //门店id
                         @BeanProperty var shopName: String, //门店名称
                         @BeanProperty var shopProvinceId: String, //门店所在省份id
                         @BeanProperty var shopProvinceName: String, //门店所在省份名称
                         @BeanProperty var shopCityId: String, //门店所在城市id
                         @BeanProperty var shopCityName: String, //门店所在城市名称
                         @BeanProperty var clientProvince: String, //客户所在省份
                         @BeanProperty var clientCity: String //客户所在城市
                       )


/**
 * 购物车宽表伴生对象
 */
object CartWideBean {
  def apply(cartBean: CartBean): CartWideBean = {

    // fixme  cartBean.addTime为Long类型.转换为指定格式的日期字符串,
    //  DateFormatUtils.format(cartBean.addTime.toLong, "yyyy-MM-dd HH:mm:ss")

    CartWideBean(
      cartBean.goodsId,
      cartBean.userId,
      cartBean.count,
      cartBean.guid,
      DateFormatUtils.format(cartBean.addTime, "yyyy-MM-dd HH:mm:ss").toString,
      cartBean.ip,
      0, "", "", "", "", "", "", "", "", "", "", "", "")

  }
}