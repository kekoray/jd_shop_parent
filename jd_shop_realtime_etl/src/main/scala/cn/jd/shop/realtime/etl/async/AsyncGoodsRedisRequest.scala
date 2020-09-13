package cn.jd.shop.realtime.etl.async


import cn.jd.shop.realtime.etl.bean.{DimGoodsCatsDBEntity, DimOrgDBEntity, DimShopCatsDBEntity, DimShopsDBEntity, GoodsWideBean}
import cn.jd.shop.realtime.etl.utils.RedisUtil
import com.jd.canal_client.bean.CanalRowData
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import redis.clients.jedis.Jedis

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

/**
 *
 * @ProjectName: jd_shop_parent  
 * @program: cn.jd.shop.realtime.etl.process   
 * @FileName: AsyncGoodsRedisRequest 
 * @description: 商品宽表的异步拉宽操作
 * @version: 1.0   
 *           *
 * @author: koray   
 * @create: 2020-08-31 17:24  
 * @Copyright (c) 2020,All Rights Reserved.
 */
class AsyncGoodsRedisRequest() extends RichAsyncFunction[CanalRowData, GoodsWideBean] {
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
   * @param rowData
   * @param resultFuture
   */
  override def asyncInvoke(rowData: CanalRowData, resultFuture: ResultFuture[GoodsWideBean]): Unit = {

    Future {
      if (!jedis.isConnected) {
        jedis = RedisUtil.getJedis()
        jedis.select(1)
      }


      var goodsWideBean: GoodsWideBean = null

      try {

        //1：根据店铺id获取店铺信息json字符串
        val shopJson = jedis.hget("jd_shop_realtime:jd_shops", rowData.getColumns.get("shopId"))
        val dimShop: DimShopsDBEntity = DimShopsDBEntity(shopJson)

        //2.1：获取商品的三级分类信息
        val thirdCatJson: String = jedis.hget("jd_shop_realtime:jd_goods_cats", rowData.getColumns.get("goodsCatId"))
        val thirdCatDbEntity: DimGoodsCatsDBEntity = DimGoodsCatsDBEntity(thirdCatJson)

        //2.2：获取商品的二级分类信息
        val secondCatJson: String = jedis.hget("jd_shop_realtime:jd_goods_cats", thirdCatDbEntity.parentId.toString)
        val secondCatDbEntity = DimGoodsCatsDBEntity(secondCatJson)

        //2.3：获取商品的一级分类信息
        val firstCatJson: String = jedis.hget("jd_shop_realtime:jd_goods_cats", secondCatDbEntity.parentId.toString)
        val firstCatDbEntity = DimGoodsCatsDBEntity(firstCatJson)

        //3：获取门店商品分类信息
        //3.1：获取门店二级商品分类信息
        val secondShopCatJson = jedis.hget("jd_shop_realtime:jd_shop_cats", rowData.getColumns.get("shopCatId2"))
        val dimSecondShopCat = DimShopCatsDBEntity(secondShopCatJson)

        //3.2：获取门店一级商品分类信息
        val firstShopCatJson = jedis.hget("jd_shop_realtime:jd_shop_cats", rowData.getColumns.get("shopCatId1"))
        val dimFirstShopCat = DimShopCatsDBEntity(firstShopCatJson)

        //4：根据店铺表的区域id找到组织机构数据
        //4.1：根据区域id找到城市数据
        val cityJson: String = jedis.hget("jd_shop_realtime:jd_org", dimShop.areaId.toString)
        val cityDbEntity: DimOrgDBEntity = DimOrgDBEntity(cityJson)

        //4.1：根据区域的父id找到大区数据
        val regionJson: String = jedis.hget("jd_shop_realtime:jd_org", cityDbEntity.parentId.toString)
        val regionDbEntity: DimOrgDBEntity = DimOrgDBEntity(regionJson)

        goodsWideBean = GoodsWideBean(rowData.getColumns.get("goodsId").toLong,
          rowData.getColumns.get("goodsSn"),
          rowData.getColumns.get("productNo"),
          rowData.getColumns.get("goodsName"),
          rowData.getColumns.get("goodsImg"),
          rowData.getColumns.get("shopId"),
          dimShop.shopName,
          rowData.getColumns.get("goodsType"),
          rowData.getColumns.get("marketPrice"),
          rowData.getColumns.get("shopPrice"),
          rowData.getColumns.get("warnStock"),
          rowData.getColumns.get("goodsStock"),
          rowData.getColumns.get("goodsUnit"),
          rowData.getColumns.get("goodsTips"),
          rowData.getColumns.get("isSale"),
          rowData.getColumns.get("isBest"),
          rowData.getColumns.get("isHot"),
          rowData.getColumns.get("isNew"),
          rowData.getColumns.get("isRecom"),
          rowData.getColumns.get("goodsCatIdPath"),
          thirdCatDbEntity.catId.toInt,
          thirdCatDbEntity.catName,
          secondCatDbEntity.catId.toInt,
          secondCatDbEntity.catName,
          firstCatDbEntity.catId.toInt,
          firstCatDbEntity.catName,
          dimFirstShopCat.getCatId.toString,
          dimFirstShopCat.catName,
          dimSecondShopCat.getCatId.toString,
          dimSecondShopCat.catName,
          rowData.getColumns.get("brandId"),
          rowData.getColumns.get("goodsDesc"),
          rowData.getColumns.get("goodsStatus"),
          rowData.getColumns.get("saleNum"),
          rowData.getColumns.get("saleTime"),
          rowData.getColumns.get("visitNum"),
          rowData.getColumns.get("appraiseNum"),
          rowData.getColumns.get("isSpec"),
          rowData.getColumns.get("gallery"),
          rowData.getColumns.get("goodsSeoKeywords"),
          rowData.getColumns.get("illegalRemarks"),
          rowData.getColumns.get("dataFlag"),
          rowData.getColumns.get("createTime"),
          rowData.getColumns.get("isFreeShipping"),
          rowData.getColumns.get("goodsSerachKeywords"),
          rowData.getColumns.get("modifyTime"),
          cityDbEntity.orgId.toInt,
          cityDbEntity.orgName,
          regionDbEntity.orgId.toInt,
          regionDbEntity.orgName)

      } catch {
        case exception: Exception => println(exception)
      }

      resultFuture.complete(Array(goodsWideBean))
    }

  }


  /**
   * 超时情况会调用的方法, 不重写的话超时会报错...
   *
   * @param input
   * @param resultFuture
   */
  override def timeout(input: CanalRowData, resultFuture: ResultFuture[GoodsWideBean]): Unit = {
    println("商品宽表实时拉宽操作的时候，与维度表的数据关联超时了")
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
