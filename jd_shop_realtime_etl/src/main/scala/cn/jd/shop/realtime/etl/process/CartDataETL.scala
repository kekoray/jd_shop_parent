package cn.jd.shop.realtime.etl.process

import cn.jd.shop.realtime.etl.`trait`.MQBaseETL
import cn.jd.shop.realtime.etl.bean.{CartBean, CartWideBean, DimGoodsCatsDBEntity, DimGoodsDBEntity, DimOrgDBEntity, DimShopsDBEntity}
import cn.jd.shop.realtime.etl.utils.{GlobalConfigUtil, RedisUtil}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import ipUtil.IPSeeker
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import redis.clients.jedis.Jedis

/**
 * 隐式转换
 */
import org.apache.flink.streaming.api.scala._

/**
 *
 * @ProjectName: jd_shop_parent
 * @program: cn.jd.shop.realtime.etl
 * @FileName: CartDataETL
 * @description: 购物车数据的实时ETL
 * @version: 1.0
 *           *
 * @author: koray
 * @create: 2020-08-31 15:14
 * @Copyright (c) 2020,All Rights Reserved.
 */
class CartDataETL(env: StreamExecutionEnvironment) extends MQBaseETL(env) {
  /**
   * 定义TEL处理操作的方法,目的是让所有的etl业务实现的时候，都继承自该方法实现其业务逻辑
   */
  override def process(): Unit = {
    //1.从Kafka中拉取购物车消息数据
    val cartDS: DataStream[String] = getKafkaDataStream(GlobalConfigUtil.`input.topic.cart`)
//    (116055, {"addTime":1598979497672,"count":1,"goodsId":"116055","guid":"5c03fbb6-1d2f-4d9f-8c75-6051622d6f8a","ip":"61.233.174.0","userId":"100208"})
    //{"addTime":"Mon Dec 16 18:01:41 CST 2019","count":1,"goodsId":"100106","guid":"fd3e6beb-2ce3-4eda-bcaa-bc6221f85016","ip":"123.125.71.102","userId":"100208"}

    //2.将购物车消息数据转换成购物车表对象
    val cartBeanDS: DataStream[CartBean] = cartDS.map(CartBean(_))

    //3.从redis中拉取维度数据,对购物车表对象进行拉宽操作,转换成购物车宽表对象返回
    val cartWideBeanDS: DataStream[CartWideBean] = cartBeanDS.map(new RichMapFunction[CartBean, CartWideBean] {
      var jedis: Jedis = null
      var ipSeeker: IPSeeker = _

      override def open(parameters: Configuration): Unit = {
        try {
          jedis = RedisUtil.getJedis()
          jedis.select(1)
        } catch {
          case exception: Exception => println(exception)
        }
        ipSeeker = new IPSeeker(getRuntimeContext.getDistributedCache.getFile("qqwry.dat"))
      }

      override def map(cartBean: CartBean): CartWideBean = {
        if (!jedis.isConnected) {
          jedis = RedisUtil.getJedis()
          jedis.select(1)
        }

        var cartWideBean: CartWideBean= null

        try {
          //  fixme 要try..catch.. 避免异常数据的格式错乱

        //转换成购物车宽表对象
          cartWideBean = CartWideBean(cartBean)

        //测试数据:
        // bin/kafka-console-producer.sh --topic ods_jd_cart  --broker-list node-1:9092
        //{"addTime":"Mon Dec 16 18:01:41 CST 2019","count":1,"goodsId":"100106","guid":"fd3e6beb-2ce3-4eda-bcaa-bc6221f85016","ip":"123.125.71.102","userId":"100208"}

          //获得商品表的数据
          val goodsJSON: String = jedis.hget("jd_shop_realtime:jd_goods", cartWideBean.goodsId)
          val dimGoods: DimGoodsDBEntity = DimGoodsDBEntity(goodsJSON)

          //获取商品三级分类数据
          val goodsCat3JSON: String = jedis.hget("jd_shop_realtime:jd_goods_cats", dimGoods.goodsCatId.toString)
          val dimgoodsCat3: DimGoodsCatsDBEntity = DimGoodsCatsDBEntity(goodsCat3JSON)

          //获取商品二级分类数据
          val goodsCat2JSON: String = jedis.hget("jd_shop_realtime:jd_goods_cats", dimgoodsCat3.parentId.toString)
          val dimgoodsCat2: DimGoodsCatsDBEntity = DimGoodsCatsDBEntity(goodsCat2JSON)

          //获取商品一级分类数据
          val goodsCat1JSON: String = jedis.hget("jd_shop_realtime:jd_goods_cats", dimgoodsCat2.parentId.toString)
          val dimgoodsCat1: DimGoodsCatsDBEntity = DimGoodsCatsDBEntity(goodsCat1JSON)

          //获取商品店铺数据
          val shopJSON: String = jedis.hget("jd_shop_realtime:jd_shops", dimGoods.shopId.toString)
          val dimShops: DimShopsDBEntity = DimShopsDBEntity(shopJSON)

          //获取店铺管理所属城市数据
          val orgCityJSON: String = jedis.hget("jd_shop_realtime:jd_org", dimShops.areaId.toString)
          val dimOrgCity: DimOrgDBEntity = DimOrgDBEntity(orgCityJSON)

          //获取店铺城市所属省份数据
          val orgProvinceJSON: String = jedis.hget("jd_shop_realtime:jd_org", dimOrgCity.parentId.toString)
          val dimOrgProvince: DimOrgDBEntity = DimOrgDBEntity(orgProvinceJSON)


          //设置商品数据
          cartWideBean.goodsPrice = dimGoods.shopPrice
          cartWideBean.goodsName = dimGoods.goodsName
          cartWideBean.goodsCat3 = dimgoodsCat3.catName
          cartWideBean.goodsCat2 = dimgoodsCat2.catName
          cartWideBean.goodsCat1 = dimgoodsCat1.catName
          cartWideBean.shopId = dimShops.shopId.toString
          cartWideBean.shopName = dimShops.shopName
          cartWideBean.shopProvinceId = dimOrgProvince.orgId.toString
          cartWideBean.shopProvinceName = dimOrgProvince.orgName
          cartWideBean.shopCityId = dimOrgCity.orgId.toString
          cartWideBean.shopCityName = dimOrgCity.orgName

          //解析IP数据
          val country: String = ipSeeker.getCountry(cartWideBean.ip)
          var areaArray: Array[String] = country.split("省")
          if (areaArray.size > 1) {
            cartWideBean.clientProvince = areaArray(0) + "省";
            cartWideBean.clientCity = areaArray(1)
          } else {
            areaArray = country.split("市");
            if (areaArray.length > 1) {
              cartWideBean.clientProvince = areaArray(0) + "市";
              cartWideBean.clientCity = areaArray(1)
            }
            else {
              cartWideBean.clientProvince = areaArray(0);
              cartWideBean.clientCity = ""
            }
          }
        } catch {
          case exception: Exception => {
            println(exception)
          }
        }

        cartWideBean
      }

      override def close(): Unit = {
        if (jedis != null && jedis.isConnected) {
          jedis.close()
        }
      }

    }
    )

   cartWideBeanDS.printToErr("拉宽后的购物车数据>>>")

   //4.将购物车宽表对象转换成JSON字符串,发送到Kafka中
   val jsonCartWideBeanDS: DataStream[String] = cartWideBeanDS.map(JSON.toJSONString(_, SerializerFeature.DisableCircularReferenceDetect))
   jsonCartWideBeanDS.addSink(kafkaProducer(GlobalConfigUtil.`output.topic.cart`))
 }
}


