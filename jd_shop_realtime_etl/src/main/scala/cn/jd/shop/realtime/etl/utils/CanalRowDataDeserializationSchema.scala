package cn.jd.shop.realtime.etl.utils

import com.jd.canal_client.bean.CanalRowData
import org.apache.flink.api.common.serialization.{AbstractDeserializationSchema}

/**
 *
 * @ProjectName: jd_shop_parent  
 * @program: cn.jd.shop.realtime.etl.trait   
 * @FileName: CanalRowDataDeserializationSchema 
 * @description: 自定义反序列化实现类，继承自AbstractDeserializationSchema抽象类
 * @version: 1.0   
 *           *
 * @author: koray   
 * @create: 2020-08-29 0:47  
 * @Copyright (c) 2020,All Rights Reserved.
 */
class CanalRowDataDeserializationSchema() extends AbstractDeserializationSchema[CanalRowData] {

  /**
   * 将kafka消费的字节码数据转换成canalRowData对象
   * @param message
   * @return
   */
  override def deserialize(message: Array[Byte]): CanalRowData = {
    //将字节码数据转换成CanalRowData对象返回
    new CanalRowData(message)
  }
}
