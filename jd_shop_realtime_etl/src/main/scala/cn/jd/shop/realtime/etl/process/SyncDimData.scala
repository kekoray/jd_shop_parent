package cn.jd.shop.realtime.etl.process

import cn.jd.shop.realtime.etl.`trait`.MysqlBaseETL
import cn.jd.shop.realtime.etl.bean.{DimGoodsCatsDBEntity, DimGoodsDBEntity, DimOrgDBEntity, DimShopCatsDBEntity, DimShopsDBEntity}
import cn.jd.shop.realtime.etl.utils.RedisUtil
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.{SerializeConfig, SerializerFeature}
import com.jd.canal_client.bean.CanalRowData
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import redis.clients.jedis.Jedis


/**
 *
 * @ProjectName: jd_shop_parent  
 * @program: cn.jd.shop.realtime.etl.process   
 * @FileName: SyncDimData 
 * @description: 所有维度数据的增量更新到redis数据库
 * @version: 1.0   
 *           *
 * @author: koray   
 * @create: 2020-08-28 23:14  
 * @Copyright (c) 2020,All Rights Reserved.
 */
class SyncDimData(env: StreamExecutionEnvironment) extends MysqlBaseETL(env) {

  /**
   * 定义TEL处理操作的方法,目的是让所有的etl业务实现的时候，都继承自该方法实现其业务逻辑
   *
   * 实现步骤：
   * 1：获取数据源
   * 2：过滤出5张表的维度数据
   * 3：处理同步过来的数据,更新到redis中
   */
  override def process() = {

    //1：获取数据源
    val canalDataStream: DataStream[CanalRowData] = getKafkaDataStream()

    canalDataStream.print("canal获得更新数据>>> ")

    //2：过滤出5张表的维度数据
    val dimRowDataStream: DataStream[CanalRowData] = canalDataStream.filter(
      rowData => {
        rowData.getTableName match {
          case "jd_goods" => true
          case "jd_goods_cats" => true
          case "jd_shops" => true
          case "jd_shop_cats" => true
          case "jd_org" => true
          //一定要加上else，否则会抛出异常
          case _ => false
        }
      }
    )


    //3：处理同步过来的数据，更新到redis中
    val value = dimRowDataStream.addSink(new RichSinkFunction[CanalRowData] {
      //定义redis的对象
      var jedis: Jedis = _

      /**
       * open方法初始化资源,只被调用一次
       *
       * @param parameters
       */
      override def open(parameters: Configuration): Unit = {
        jedis = RedisUtil.getJedis()
        //指定维度数据所在的数据库
        jedis.select(1)
      }


      /**
       * 逻辑处理的函数，一条条的处理数据
       *
       * @param rowData
       * @param context
       */
      override def invoke(rowData: CanalRowData, context: SinkFunction.Context[_]): Unit = {
        //根据操作类型的不同，调用不同的业务逻辑实现数据的更新
        rowData.getEventType match {
          case eventTpye if (eventTpye == "insert" || eventTpye == "update") => {
            println("----insert/update操作----")
            updateDimData(rowData)
          }
          case "delete" => {
            println("----delete操作----")
            deleteDimData(rowData)
          }
          case _ =>
        }

      }


      /**
       * 关闭连接，释放资源
       */
      override def close(): Unit = {
        //如果当前redis是连接状态，则关闭连接，释放连接
        if (!jedis.isConnected) jedis.close()
      }


      /**
       * 更新维度数据
       *
       * @param rowData
       */
      def updateDimData(rowData: CanalRowData): Unit = {
        rowData.getTableName match {

          /**
           * 商品维度表更新
           */
          case "jd_goods" => {
            val goodsId: Long = rowData.getColumns.get("goodsId").toLong
            val goodsName: String = rowData.getColumns.get("goodsName")
            val shopId: Long = rowData.getColumns.get("shopId").toLong
            val goodsCatId: Int = rowData.getColumns.get("goodsCatId").toInt
            val shopPrice: Double = rowData.getColumns.get("shopPrice").toDouble

            //获得更新的数据封装到bean对象中
            val goodsDBEntity: DimGoodsDBEntity = new DimGoodsDBEntity(goodsId, goodsName, shopId, goodsCatId, shopPrice)

            //转换json字符串,关闭循环引用
            val goodsJson = JSON.toJSONString(goodsDBEntity, SerializerFeature.DisableCircularReferenceDetect)

            //更新数据到redis中
            jedis.hset("jd_shop_realtime:jd_goods", goodsId.toString, goodsJson)
          }


          /**
           * 店铺维度数据更新
           */
          case "jd_shops" => {
            val shopId = rowData.getColumns.get("shopId").toLong
            val areaId = rowData.getColumns.get("areaId").toLong
            val shopName = rowData.getColumns.get("shopName").toString
            val shopCompany = rowData.getColumns.get("shopCompany").toString

            val ShopsDBEntity: DimShopsDBEntity = DimShopsDBEntity(shopId, areaId, shopName, shopCompany)
            val shopsjson = JSON.toJSONString(ShopsDBEntity, SerializerFeature.DisableCircularReferenceDetect)
            jedis.hset("jd_shop_realtime:jd_shops", shopId.toString, shopsjson)
          }


          /**
           * 商品分类维度数据更新
           */
          case "jd_goods_cats"
          => {
            val catId = rowData.getColumns.get("catId").toLong
            val parentId = rowData.getColumns.get("parentId").toLong
            val catName = rowData.getColumns.get("catName")
            val cat_level = rowData.getColumns.get("cat_level")

            val goodsCatsDBEntity = new DimGoodsCatsDBEntity(catId, parentId, catName, cat_level)
            val goodsCatsJson = JSON.toJSONString(goodsCatsDBEntity, SerializerFeature.DisableCircularReferenceDetect)
            jedis.hset("jd_shop_realtime:jd_goods_cats", catId.toString, goodsCatsJson)
          }

          /**
           * 门店商品分类维度数据更新
           */
          case "jd_shop_cats"
          => {
            val catId = rowData.getColumns.get("catId").toLong
            val parentId = rowData.getColumns.get("parentId").toLong
            val catName = rowData.getColumns.get("catName")
            val catSort = rowData.getColumns.get("catSort")

            val shopCatsDBEntity = new DimShopCatsDBEntity(catId, parentId, catName, catSort)
            val shopCatsJson = JSON.toJSONString(shopCatsDBEntity, SerializerFeature.DisableCircularReferenceDetect)
            jedis.hset("jd_shop_realtime:jd_shop_cats", catId.toString, shopCatsJson)
          }

          /**
           * 组织机构维度数据更新
           */
          case "jd_org"
          => {
            val orgId = rowData.getColumns.get("orgId").toLong
            val parentId = rowData.getColumns.get("parentId").toLong
            val orgName = rowData.getColumns.get("orgName")
            val orgLevel: String = rowData.getColumns.get("orgLevel")

            val orgDBEntity = DimOrgDBEntity(orgId, parentId, orgName, orgLevel)
            val orgJson = JSON.toJSONString(orgDBEntity, SerializerFeature.DisableCircularReferenceDetect)
            jedis.hset("jd_shop_realtime:jd_org", orgId + "", orgJson)
          }
        }
      }


      /**
       * 删除维度数据
       *
       * @param rowData
       */
      def deleteDimData(rowData: CanalRowData): Unit = {
        rowData.getTableName match {
          /**
           * //删除商品维度表的数据
           */
          case "jd_goods" => {
            jedis.hdel("jd_goods", rowData.getColumns.get("goodsId"))
          }

          /**
           * 删除店铺维度表的数据
           */
          case "" => {
            jedis.hdel("jd_shops", rowData.getColumns.get("shopId"))
          }

          /**
           * 删除商品分类维度数据
           */
          case "" => {
            jedis.hdel("jd_goods_cats", rowData.getColumns.get("catId"))
          }

          /**
           * 删除门店商品分类维度数据
           */
          case "jd_shop_cats" => {
            jedis.hdel("jd_shop_cats", rowData.getColumns.get("catId"))
          }

          /**
           * 删除组织机构维度数据
           */
          case "jd_org" => {
            jedis.hdel("jd_shop_realtime:jd_org", rowData.getColumns.get("orgId"))
          }
        }
      }
    })

  }

}
