package cn.jd.shop.realtime.etl.bean

import com.alibaba.fastjson.{JSON, JSONObject}
import lombok.Setter

import scala.beans.BeanProperty

/**
 * @ProjectName: jd_shop_parent  
 * @program: cn.jd.shop.realtime.etl.dataloader
 * @FileName: DBEntity
 * @description: 定义维度表的样例类
 * @version: 1.0
 * @author: koray
 * @create: 2020-08-28 20:59
 * @Copyright (c) 2020,All Rights Reserved.
 */


/**
 * 商品维度样例类
 *
 * @BeanProperty 生成set和get方法
 *               如果不加@BeanProperty标签，意味着不会生成setter/getter方法，
 *               因此在对样例类序列化的时候，会丢失这个属性
 * @param goodsId    // 商品id
 * @param goodsName  // 商品名称
 * @param shopId     // 店铺id
 * @param goodsCatId // 商品分类id
 * @param shopPrice  // 商品价格
 */
case class DimGoodsDBEntity(@BeanProperty goodsId: Long = 0,
                            @BeanProperty goodsName: String = "",
                            @BeanProperty shopId: Long = 0,
                            @BeanProperty goodsCatId: Int = 0,
                            @BeanProperty shopPrice: Double = 0)

/**
 * 商品的伴生对象
 */
object DimGoodsDBEntity {
  def apply(json: String): DimGoodsDBEntity = {

    //正常的话，订单明细表的商品id会存在与商品表中，假如商品id不存在商品表，这里解析的时候就会抛出异常
    if (json != null) {
      val jSONObject: JSONObject = JSON.parseObject(json)
      new DimGoodsDBEntity(
        jSONObject.getLong("goodsId"),
        jSONObject.getString("goodsName"),
        jSONObject.getLong("shopId"),
        jSONObject.getInteger("goodsCatId"),
        jSONObject.getDouble("shopPrice")
      )
    } else {
      new DimGoodsDBEntity
    }
  }
}

/**
 * 店铺维度样例类
 *
 * @param shopId      // 店铺id
 * @param areaId      // 店铺所属区域id
 * @param shopName    // 店铺名称
 * @param shopCompany // 公司名称
 */
case class DimShopsDBEntity(@BeanProperty var shopId: Long = 0,
                            @BeanProperty var areaId: Long = 0,
                            @BeanProperty var shopName: String = "",
                            @BeanProperty var shopCompany: String = "")

/**
 * 店铺维度伴生对象
 */
object DimShopsDBEntity {
  def apply(json: String): DimShopsDBEntity = {

    if (json != null) {
      val jSONObject = JSON.parseObject(json)
      new DimShopsDBEntity(
        jSONObject.getLong("shopId"),
        jSONObject.getLong("areaId"),
        jSONObject.getString("shopName"),
        jSONObject.getString("shopCompany")
      )
    } else {
      new DimShopsDBEntity()
    }
  }
}


/**
 * 商品分类维度样例类
 *
 * @param catId     // 商品分类id
 * @param parentId  // 商品分类父id
 * @param catName   // 商品分类名称
 * @param cat_level // 商品分类级别
 */
case class DimGoodsCatsDBEntity(@BeanProperty var catId: Long = 0,
                                @BeanProperty var parentId: Long = 0,
                                @BeanProperty var catName: String = "",
                                @BeanProperty var cat_level: String = "")

/**
 * 商品分类维度样例类
 */
object DimGoodsCatsDBEntity {
  def apply(json: String): DimGoodsCatsDBEntity = {
    if (json != null) {
      val jSONObject = JSON.parseObject(json)
      new DimGoodsCatsDBEntity(
        jSONObject.getLong("catId"),
        jSONObject.getLong("parentId"),
        jSONObject.getString("catName"),
        jSONObject.getString("cat_level")
      )
    } else {
      new DimGoodsCatsDBEntity()
    }
  }
}


/**
 * 机构维度样例表
 *
 * @param orgId    // 机构id
 * @param parentId // 机构父id
 * @param orgName  // 组织机构名称
 * @param orgLevel // 组织机构级别
 */
case class DimOrgDBEntity(@BeanProperty var orgId: Long = 0,
                          @BeanProperty var parentId: Long = 0,
                          @BeanProperty var orgName: String = "",
                          @BeanProperty var orgLevel: String = "")

/**
 * 机构维度伴生对象
 */
object DimOrgDBEntity {
  def apply(json: String): DimOrgDBEntity = {
    if (json != null) {
      val jSONObject = JSON.parseObject(json)
      new DimOrgDBEntity(
        jSONObject.getLong("orgId"),
        jSONObject.getLong("parentId"),
        jSONObject.getString("orgName"),
        jSONObject.getString("orgLevel"))
    } else {
      new DimOrgDBEntity()
    }
  }
}


/**
 * 门店商品分类维度样例表
 *
 * @param catId    // 商品分类id
 * @param parentId // 商品分类父id
 * @param catName  // 商品分类名称
 * @param catSort  // 商品分类级别
 */
case class DimShopCatsDBEntity(@BeanProperty var catId: Long = 0,
                               @BeanProperty var parentId: Long = 0,
                               @BeanProperty var catName: String = "",
                               @BeanProperty var catSort: String = ""
                              )

/**
 * 门店商品分类维度伴生对象
 */
object DimShopCatsDBEntity {
  def apply(json: String): DimShopCatsDBEntity = {
    if (json != null) {
      val jSONObject = JSON.parseObject(json)
      new DimShopCatsDBEntity(
        jSONObject.getLong("catId"),
        jSONObject.getLong("parentId"),
        jSONObject.getString("catName"),
        jSONObject.getString("catSort")
      )
    } else {
      new DimShopCatsDBEntity()
    }
  }
}