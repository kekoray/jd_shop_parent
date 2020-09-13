package cn.jd.shop.realtime.etl.async

import cn.jd.shop.realtime.etl.bean.{DimGoodsCatsDBEntity, DimGoodsDBEntity, DimOrgDBEntity, DimShopsDBEntity, OrderGoodsWideEntity}
import cn.jd.shop.realtime.etl.utils.RedisUtil
import com.jd.canal_client.bean.CanalRowData
import org.apache.flink.configuration.Configuration
import redis.clients.jedis.Jedis
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

/**
 *
 * @ClassName: AsyncOrderDetailRedisRequest
 * @description: 订单明细表的异步拉宽操作
 * @param: * @param null
 * @return:
 * @author: koray
 * @create: 2020/8/31
 * @since: JDK 1.8
 */
class AsyncOrderDetailRedisRequest extends RichAsyncFunction[CanalRowData, OrderGoodsWideEntity] {

  //定义redis的连接对象
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
  implicit lazy val excutor: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.directExecutor())

  /**
   * 异步处理每条数据
   *
   * @param rowData
   * @param resultFuture
   */
  override def asyncInvoke(rowData: CanalRowData, resultFuture: ResultFuture[OrderGoodsWideEntity]): Unit = {

    //发起异步请求
    Future {
      if (!jedis.isConnected) {
        jedis = RedisUtil.getJedis()
        jedis.select(1)
      }

      println("begin>>>" + rowData.toString)
      //1.根据商品id获取商品的详细信息,并将其反序列化成样例类
      val goodsJson: String = jedis.hget("jd_shop_realtime:jd_goods", rowData.getColumns.get("goodsId"))
      val goodsDBEntity: DimGoodsDBEntity = DimGoodsDBEntity(goodsJson)

      //2.根据商品表的店铺id获取店铺信息,并将其反序列化成样例类
      val shopsJson: String = jedis.hget("jd_shop_realtime:jd_shops", goodsDBEntity.shopId.toString)
      val shopsDBEntity: DimShopsDBEntity = DimShopsDBEntity(shopsJson)

      //3.根据商品表的商品分类id获取商品的分类信息
      //3.1: 获取商品的三级分类信息
      val thirdCatJson: String = jedis.hget("jd_shop_realtime:jd_goods_cats", goodsDBEntity.goodsCatId.toString)
      val thirdCatDBEntity: DimGoodsCatsDBEntity = DimGoodsCatsDBEntity(thirdCatJson)

      //3.2: 获取商品的二级分类信息
      val secondCatJson: String = jedis.hget("jd_shop_realtime:jd_goods_cats", thirdCatDBEntity.parentId.toString)
      val secondCatDBEntity: DimGoodsCatsDBEntity = DimGoodsCatsDBEntity(secondCatJson)

      //3.3: 获取商品的一级分类信息
      val firstCatJson: String = jedis.hget("jd_shop_realtime:jd_goods_cats", secondCatDBEntity.parentId.toString)
      val firstCatDbEntity: DimGoodsCatsDBEntity = DimGoodsCatsDBEntity(firstCatJson)

      //4.根据店铺表的区域id找到组织机构数据
      //4.1 根据区域id找到城市数据
      val cityJson: String = jedis.hget("jd_shop_realtime:jd_org", shopsDBEntity.areaId.toString)
      val cityDbEntity: DimOrgDBEntity = DimOrgDBEntity(cityJson)

      //4.2 根据区域的父id找到大区数据
      val regionJson: String = jedis.hget("jd_shop_realtime:jd_org", cityDbEntity.parentId.toString)
      val regionDbEntity: DimOrgDBEntity = DimOrgDBEntity(regionJson)

      println("end>>>" + rowData.toString)


      val orderGoodsWideEntity: OrderGoodsWideEntity = OrderGoodsWideEntity(
        rowData.getColumns.get("ogId").toLong,
        rowData.getColumns.get("orderId").toLong,
        rowData.getColumns.get("goodsId").toLong,
        rowData.getColumns.get("goodsNum").toLong,
        rowData.getColumns.get("goodsPrice").toDouble,
        rowData.getColumns.get("goodsName"),
        shopsDBEntity.shopId,
        thirdCatDBEntity.catId.toInt,
        thirdCatDBEntity.catName,
        secondCatDBEntity.catId.toInt,
        secondCatDBEntity.catName,
        firstCatDbEntity.catId.toInt,
        firstCatDbEntity.catName,
        shopsDBEntity.areaId.toInt,
        shopsDBEntity.shopName,
        shopsDBEntity.shopCompany,
        cityDbEntity.orgId.toInt,
        cityDbEntity.orgName,
        regionDbEntity.orgId.toInt,
        regionDbEntity.orgName
      )


      resultFuture.complete(Array(orderGoodsWideEntity))

    }

  }


  /**
   * 关闭redis连接
   */
  override def close(): Unit = {
    try {
      if (jedis.isConnected) {
        jedis.close()
      }
    } catch {
      case exception: Exception => println(exception)
    }
  }

  /**
   * 超时情况会调用的方法, 不重写的话超时会报错...
   *
   * @param input
   * @param resultFuture
   */
  override def timeout(input: CanalRowData, resultFuture: ResultFuture[OrderGoodsWideEntity]): Unit = {
    println("订单明细实时拉宽操作的时候，与维度表的数据关联超时了")
  }
}