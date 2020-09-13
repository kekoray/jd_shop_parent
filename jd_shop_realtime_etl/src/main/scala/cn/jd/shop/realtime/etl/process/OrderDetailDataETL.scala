package cn.jd.shop.realtime.etl.process

import java.util.concurrent.TimeUnit

import cn.jd.shop.realtime.etl.`trait`.MysqlBaseETL
import cn.jd.shop.realtime.etl.async.AsyncOrderDetailRedisRequest
import cn.jd.shop.realtime.etl.bean.OrderGoodsWideEntity
import cn.jd.shop.realtime.etl.utils.pool.HbaseConnectionPool
import cn.jd.shop.realtime.etl.utils.{GlobalConfigUtil, HbaseUtil}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.jd.canal_client.bean.CanalRowData
import org.apache.flink.configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, Put, Table}
import org.apache.hadoop.hbase.util.Bytes


/**
 *
 * @ProjectName: jd_shop_parent  
 * @program: cn.jd.shop.realtime.etl
 * @FileName: OrderDetailDataETL
 * @description: 订单明细表的实时TEL操作
 * @version: 1.0
 *           *
 * @author: koray
 * @create: 2020-08-31 12:46
 * @Copyright (c) 2020,All Rights Reserved.
 */
class OrderDetailDataETL(env: StreamExecutionEnvironment) extends MysqlBaseETL(env) {
  /**
   * 定义TEL处理操作的方法,目的是让所有的etl业务实现的时候，都继承自该方法实现其业务逻辑
   * * 1: 获取canal中的订单明细数据，过滤出来订单明细表的数据，将CanalRowData对象转换成订单明细的宽表样例类
   * * 2: 将订单明细表的数据进行实时的拉宽（需要与redis进行交互，使用异步io实现）
   * * 3: 将拉宽后的订单明细数据转换成json后写入到kakfa集群，供druid进行实时的摄取
   * * 4: 将拉宽后的订单明细数据写入到hbase中，供后续订单明细数据的查询
   */
  override def process(): Unit = {
    //1.接入Kafka数据源, 过滤订单明细数据
    val canalRowDataStream: DataStream[CanalRowData] = getKafkaDataStream().filter(_.getTableName == "jd_order_goods")

    canalRowDataStream.print("订单明细数据>>>")

    //2.使用异步io方式,将订单明细表与redis的维度数据进行实时的拉宽,返回订单明细宽表对象
    // ** @param input 需要进行异步操作的流
    // ** @param asyncFunction 异步操作核心处理方法
    // ** @param timeout 超时时间
    // ** @param timeUnit 超时时间的单位
    // ** @param capacity  异步操作的数量
    val orderGoodsWideDataStream: DataStream[OrderGoodsWideEntity] = AsyncDataStream.unorderedWait(canalRowDataStream, new AsyncOrderDetailRedisRequest(), 100,
      TimeUnit.SECONDS, 100)

    orderGoodsWideDataStream.printToErr("拉宽后的订单明细数据>>>")

    //3.将订单明细宽表对象转换成json字符串,发送到Kafka中
        val jsonOrderGoodsWideDataStream = orderGoodsWideDataStream.map(JSON.toJSONString(_, SerializerFeature.DisableCircularReferenceDetect))
        jsonOrderGoodsWideDataStream.addSink(kafkaProducer(GlobalConfigUtil.`output.topic.order_detail`))


    // todo ---------------------------------------------
    //OrderGoodsWideEntity(1,1,113886,123,5999.0,,104537,351,手机,348,手机通讯,334,手机数码、家用电器,100117,迪信通官方旗舰店,迪信通官方旗舰店,100117,西双版纳傣族自治州分公司,100007,华西大区)


    //4.将订单明细宽表储存到hbase中
    orderGoodsWideDataStream.addSink(new RichSinkFunction[OrderGoodsWideEntity] {

      //定义hbase的连接对象
      var connection: Connection = _
      //定义hbase的表
      var table: Table = _


      /**
       *
       * @param parameters
       */
      override def open(parameters: configuration.Configuration): Unit = {

        //获得连接池对象
        val hbaseConnectionPool: HbaseConnectionPool = HbaseUtil.getPool()
        //获得连接对象
        connection = hbaseConnectionPool.getConnection

        val admin: Admin = connection.getAdmin

        //判断是否存在就创建表
        if (!admin.tableExists(TableName.valueOf(GlobalConfigUtil.`hbase.table.orderdetail`))) {
          val hTableDescriptor = new HTableDescriptor(TableName.valueOf(GlobalConfigUtil.`hbase.table.orderdetail`))
          val family: HColumnDescriptor = new HColumnDescriptor(GlobalConfigUtil.`hbase.table.family`)
          hTableDescriptor.addFamily(family)
          admin.createTable(hTableDescriptor)
          println("创建表成功...")

          //初始化要写入的表
          table = connection.getTable(TableName.valueOf(GlobalConfigUtil.`hbase.table.orderdetail`))
        } else {
          //初始化要写入的表
          table = connection.getTable(TableName.valueOf(GlobalConfigUtil.`hbase.table.orderdetail`))
        }
      }

      /**
       * 对数据一条条的处理, 写入到hbase中
       *
       * @param orderGoodsWideEntity
       * @param context
       */
      override def invoke(orderGoodsWideEntity: OrderGoodsWideEntity, context: SinkFunction.Context[_]): Unit = {

        try {
          //构建put对象
          //使用订单明细表id作为rowkey存储, 因为id是唯一的
          val put: Put = new Put(Bytes.toBytes(orderGoodsWideEntity.getOgId))

          //创建列族
          val family: Array[Byte] = Bytes.toBytes(GlobalConfigUtil.`hbase.table.family`)

          //将列及列的值加入到列族中
          put.addColumn(family, Bytes.toBytes("ogId"), Bytes.toBytes(orderGoodsWideEntity.ogId))
          put.addColumn(family, Bytes.toBytes("orderId"), Bytes.toBytes(orderGoodsWideEntity.orderId.toString))
          put.addColumn(family, Bytes.toBytes("goodsId"), Bytes.toBytes(orderGoodsWideEntity.goodsId.toString))
          put.addColumn(family, Bytes.toBytes("goodsNum"), Bytes.toBytes(orderGoodsWideEntity.goodsNum.toString))
          put.addColumn(family, Bytes.toBytes("goodsPrice"), Bytes.toBytes(orderGoodsWideEntity.goodsPrice.toString))
          put.addColumn(family, Bytes.toBytes("goodsName"), Bytes.toBytes(orderGoodsWideEntity.goodsName.toString))
          put.addColumn(family, Bytes.toBytes("shopId"), Bytes.toBytes(orderGoodsWideEntity.shopId.toString))
          put.addColumn(family, Bytes.toBytes("goodsThirdCatId"), Bytes.toBytes(orderGoodsWideEntity.goodsThirdCatId.toString))
          put.addColumn(family, Bytes.toBytes("goodsThirdCatName"), Bytes.toBytes(orderGoodsWideEntity.goodsThirdCatName.toString))
          put.addColumn(family, Bytes.toBytes("goodsSecondCatId"), Bytes.toBytes(orderGoodsWideEntity.goodsSecondCatId.toString))
          put.addColumn(family, Bytes.toBytes("goodsSecondCatName"), Bytes.toBytes(orderGoodsWideEntity.goodsSecondCatName.toString))
          put.addColumn(family, Bytes.toBytes("goodsFirstCatId"), Bytes.toBytes(orderGoodsWideEntity.goodsFirstCatId.toString))
          put.addColumn(family, Bytes.toBytes("goodsFirstCatName"), Bytes.toBytes(orderGoodsWideEntity.goodsFirstCatName.toString))
          put.addColumn(family, Bytes.toBytes("areaId"), Bytes.toBytes(orderGoodsWideEntity.areaId.toString))
          put.addColumn(family, Bytes.toBytes("shopName"), Bytes.toBytes(orderGoodsWideEntity.shopName.toString))
          put.addColumn(family, Bytes.toBytes("shopCompany"), Bytes.toBytes(orderGoodsWideEntity.shopCompany.toString))
          put.addColumn(family, Bytes.toBytes("cityId"), Bytes.toBytes(orderGoodsWideEntity.cityId.toString))
          put.addColumn(family, Bytes.toBytes("cityName"), Bytes.toBytes(orderGoodsWideEntity.cityName.toString))
          put.addColumn(family, Bytes.toBytes("regionId"), Bytes.toBytes(orderGoodsWideEntity.regionId.toString))
          put.addColumn(family, Bytes.toBytes("regionName"), Bytes.toBytes(orderGoodsWideEntity.regionName.toString))

          //添加数据
          table.put(put)

        } catch {
          case exception: Exception => (exception)
        }

        //执行put操作
      }


      //关闭连接，释放资源
      override def close(): Unit = {
        try {
          if (!connection.isClosed) {
            //将连接放回到连接池中

            HbaseUtil.getPool().returnConnection(connection)
          }
        } catch {
          case ex => println(ex)
        }

        //        try {
        //          if (!connection.isClosed) {
        //            connection.close()
        //          }
        //        } catch {
        //          case exception: Exception => println(exception)
        //        }

        if (table != null) {
          table.close()
        }
      }
    }
    )
  }
}
