import cn.jd.shop.realtime.etl.utils.{GlobalConfigUtil, HbaseUtil}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Put, Table}
import org.apache.hadoop.hbase.util.Bytes


/**
 *
 * @ProjectName: jd_shop_parent  
 * @program:
 * @FileName: hbasetest 
 * @description: TODO   
 * @version: 1.0   
 *           *
 * @author: koray   
 * @create: 2020-09-02 16:59  
 * @Copyright (c) 2020,All Rights Reserved.
 */
object hbasetest {
  def main(args: Array[String]): Unit = {
    val pool = HbaseUtil.getPool()
    val connection = pool.getConnection

    //    val admin = connection.getAdmin
    //    val hTableDescriptor = new HTableDescriptor(TableName.valueOf("test-hbase"))
    //    val f1 = new HColumnDescriptor("aa")
    //    val f2 = new HColumnDescriptor("bb")
    //    hTableDescriptor.addFamily(f1)
    //    hTableDescriptor.addFamily(f2)
    //    admin.createTable(hTableDescriptor)



    val table: Table = connection.getTable(TableName.valueOf(GlobalConfigUtil.`hbase.table.orderdetail`))

    //构建put对象
    //使用订单明细表id作为rowkey存储, 因为id是唯一的
    val put: Put = new Put("1".getBytes())

    //创建列族
    val family: Array[Byte] = Bytes.toBytes(GlobalConfigUtil.`hbase.table.family`)

    //将列及列的值加入到列族中
    put.addColumn(family, Bytes.toBytes("ogId"), Bytes.toBytes(1))
    put.addColumn(family, Bytes.toBytes("orderId"), Bytes.toBytes(2))
    table.put(put)

    table.close()
    connection.close()


  }


}
