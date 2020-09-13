package cn.jd.shop.realtime.etl.dataloader

import java.sql.{Connection, DriverManager}

import cn.jd.shop.realtime.etl.bean.{DimGoodsCatsDBEntity, DimGoodsDBEntity, DimOrgDBEntity, DimShopCatsDBEntity, DimShopsDBEntity}
import cn.jd.shop.realtime.etl.utils.{GlobalConfigUtil, RedisUtil}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import redis.clients.jedis.Jedis

/**
 *
 * @ProjectName: jd_shop_parent  
 * @program: cn.jd.shop.realtime.etl.dataloader   
 * @FileName: DimentsionDataLoader 
 * @description: 将历史维度数据通过离线程序同步到redis服务器
 *               五个维度表的数据需要同步到redis
 *               1）商品维度表        jd_goods
 *               2）商品分类维度表     jd_goods_cats
 *               3）店铺表            jd_shops
 *               4）门店商品分类表    jd_shop_cats
 *               5）组织机构表         jd_org
 * @version: 1.0   
 *           *
 * @author: koray   
 * @create: 2020-08-28 20:45  
 * @Copyright (c) 2020,All Rights Reserved.
 */
object DimentsionDataLoader {

  def main(args: Array[String]): Unit = {
    //1.创建MySQL驱动
    Class.forName("com.mysql.jdbc.Driver")

    //2.获得连接
    val connection = DriverManager.getConnection(s"jdbc:mysql://${GlobalConfigUtil.`mysql.server.ip`}:${GlobalConfigUtil.`mysql.server.port`}/${GlobalConfigUtil.`mysql.server.database`}",
      GlobalConfigUtil.`mysql.server.username`, GlobalConfigUtil.`mysql.server.password`)

    //3.创建redis连接
    val jedis = RedisUtil.getJedis()
    //redis中默认有16个数据库，需要指定一个维度数据存储的数据库中，默认是保存在第一个数据库中
    jedis.select(1)

    //4.加载维度表的数据到reids中
    LoadDimGoods(connection, jedis)
    loadDimShops(connection, jedis)
    LoadDimShopCats(connection, jedis)
    LoadDimGoodsCats(connection, jedis)
    LoadDimOrg(connection, jedis)

    //5.关闭连接
    jedis.close()
    connection.close()
  }


  /**
   * 加载商品维度的数据
   *
   * @param connection
   * @param jedis
   */
  def LoadDimGoods(connection: Connection, jedis: Jedis): Unit = {
    val sql =
      """
        |select
        | `goodsId`,
        | `goodsName`,
        | `shopId`,
        | `goodsCatId`,
        | `shopPrice`
        |from
        |jd_shop_realtime.jd_goods
        |""".stripMargin

    //创建statement
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)

    //遍历商品表的数据
    while (resultSet.next()) {
      val goodsId = resultSet.getLong("goodsId")
      val goodsName = resultSet.getString("goodsName")
      val shopId = resultSet.getLong("shopId")
      val goodsCatId = resultSet.getInt("goodsCatId")
      val shopPrice = resultSet.getDouble("shopPrice")

      //将获取到的商品数据封装到bean对象中
      val goodsDBEntity: DimGoodsDBEntity = DimGoodsDBEntity(goodsId, goodsName, shopId, goodsCatId, shopPrice)

      //redis是一个k/v数据库,将goodsDBEntity样例类转换成json字符串,作为redis的value写入到redis.
      //SerializerFeature.DisableCircularReferenceDetect ==> 关闭循环引用,否则会报错
      val goodsJson = JSON.toJSONString(goodsDBEntity, SerializerFeature.DisableCircularReferenceDetect)
      println("jd_shop_realtime.jd_goods>>> " + goodsJson)

      //利用hash数据结构存储到redis中, hash数据结构有3个参数可传递(表名,key,value),可用来区分表的类型
      jedis.hset("jd_shop_realtime:jd_goods", goodsId.toString, goodsJson)
    }

    statement.close()
    resultSet.close()
  }

  /**
   * 加载商铺维度数据到Redis
   *
   * @param connection
   * @param jedis
   */
  def loadDimShops(connection: Connection, jedis: Jedis): Unit = {
    val sql =
      """
        |SELECT
        |	`shopId`,
        |	`areaId`,
        |	`shopName`,
        |	`shopCompany`
        |FROM
        |jd_shop_realtime.jd_shops
        |""".stripMargin

    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)

    while (resultSet.next()) {
      val shopId = resultSet.getLong("shopId")
      val areaId = resultSet.getLong("areaId")
      val shopName = resultSet.getString("shopName")
      val shopCompany = resultSet.getString("shopCompany")

      val ShopsDBEntity: DimShopsDBEntity = DimShopsDBEntity(shopId, areaId, shopName, shopCompany)

      //转为json字符串,关闭循环引用
      val shopsjson = JSON.toJSONString(ShopsDBEntity, SerializerFeature.DisableCircularReferenceDetect)

      println("jd_shop_realtime.jd_shops>>> " + shopsjson)

      jedis.hset("jd_shop_realtime:jd_shops", shopId.toString, shopsjson)
    }

    statement.close()
    resultSet.close()
  }


  /**
   * 加载门店商品分类维度数据到redis中
   *
   * @param connection
   * @param jedis
   * @return
   */
  def LoadDimShopCats(connection: Connection, jedis: Jedis) = {
    val sql =
      """
        |select
        |`catId`,
        |`parentId`,
        |`catName`,
        |`catSort`
        |from
        |jd_shop_realtime.jd_shop_cats
        |""".stripMargin

    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)
    while (resultSet.next()) {
      val catId = resultSet.getLong("catId")
      val parentId = resultSet.getLong("parentId")
      val catName = resultSet.getString("catName")
      val catSort = resultSet.getString("catSort")

      val shopCatsDBEntity = new DimShopCatsDBEntity(catId, parentId, catName, catSort)
      val shopCatsJson = JSON.toJSONString(shopCatsDBEntity, SerializerFeature.DisableCircularReferenceDetect)

      println("jd_shop_realtime.jd_shop_cats>>> " + shopCatsJson)

      jedis.hset("jd_shop_realtime:jd_shop_cats", catId.toString, shopCatsJson)
    }

    statement.close()
    resultSet.close()
  }


  /**
   * 加载商品分类维度数据到redis中
   *
   * @param connection
   * @param jedis
   */
  def LoadDimGoodsCats(connection: Connection, jedis: Jedis) = {
    val sql =
      """
        |select
        |`catId`,
        |`parentId`,
        |`catName`,
        |`cat_level`
        |from
        |jd_shop_realtime.jd_goods_cats
        |""".stripMargin

    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)

    while (resultSet.next()) {
      val catId = resultSet.getLong("catId")
      val parentId = resultSet.getLong("parentId")
      val catName = resultSet.getString("catName")
      val cat_level = resultSet.getString("cat_level")
      val goodsCatsDBEntity = new DimGoodsCatsDBEntity(catId, parentId, catName, cat_level)
      val goodsCatsJson = JSON.toJSONString(goodsCatsDBEntity, SerializerFeature.DisableCircularReferenceDetect)

      println("jd_shop_realtime.jd_goods_cats>>> " + goodsCatsJson)

      jedis.hset("jd_shop_realtime:jd_goods_cats", catId.toString, goodsCatsJson)
    }

    statement.close()
    resultSet.close()
  }


  /**
   * 加载组织机构维度数据到redis中
   *
   * @param connection
   * @param jedis
   */
  def LoadDimOrg(connection: Connection, jedis: Jedis): Unit = {
    val sql =
      """
        |SELECT
        |`orgid`,
        |`parentid`,
        |`orgName`,
        |`orgLevel`
        |FROM
        |jd_shop_realtime.jd_org
              """.stripMargin

    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)

    while (resultSet.next()) {
      val orgId = resultSet.getLong("orgId")
      val parentId = resultSet.getLong("parentId")
      val orgName = resultSet.getString("orgName")
      val orgLevel = resultSet.getString("orgLevel")

      val orgDBEntity = DimOrgDBEntity(orgId, parentId, orgName, orgLevel)
      val orgJson = JSON.toJSONString(orgDBEntity, SerializerFeature.DisableCircularReferenceDetect)

      println("jd_shop_realtime.jd_org>>> " + orgJson)

      jedis.hset("jd_shop_realtime:jd_org", orgId + "", orgJson)
    }

    statement.close()
    resultSet.close()
  }


}
