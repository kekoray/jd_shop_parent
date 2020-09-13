package cn.jd.shop.realtime.etl.process

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import cn.jd.shop.realtime.etl.`trait`.MQBaseETL
import cn.jd.shop.realtime.etl.bean.{ClickLogEntity, ClickLogWideEntity}
import cn.jd.shop.realtime.etl.utils.GlobalConfigUtil
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import ipUtil.IPSeeker
import nl.basjes.parse.httpdlog.HttpdLoglineParser
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * 手动导入隐式转换
 */
import org.apache.flink.streaming.api.scala._

/**
 *
 * @ProjectName: jd_shop_parent  
 * @program: cn.jd.shop.realtime.etl
 * @FileName: ClickLogETL 
 * @descripti :  点击流数据的实时ETL
 * @version: 1.0   
 *           *
 * @author: koray   
 * @create: 2020-08-29 19:54  
 * @Copyright (c) 2020,All Rights Reserved.
 */
class ClickLogDataETL(env: StreamExecutionEnvironment) extends MQBaseETL(env) {

  /**
   * 定义TEL处理操作的方法,目的是让所有的etl业务实现的时候，都继承自该方法实现其业务逻辑
   */
  override def process(): Unit = {
    //1.从Kafka中获取日志数据
    val clicklogDataStream: DataStream[String] = getKafkaDataStream(GlobalConfigUtil.`input.topic.click_log`)

    //2.将日志数据进行转换,拉宽操作
    val clickLogWideEntityDataStream: DataStream[ClickLogWideEntity] = etl(clicklogDataStream)

    clickLogWideEntityDataStream.printToErr("拉宽后的点击流对象>>>")

    //3.将拉宽后的点击流对象转换成json格式
    val jsonClickLogDataStream: DataStream[String] = clickLogWideEntityDataStream.map(JSON.toJSONString(_, SerializerFeature.DisableCircularReferenceDetect))

    //4.将json字符串写入到kafka的集群
    jsonClickLogDataStream.addSink(kafkaProducer(GlobalConfigUtil.`output.topic.clicklog`))
  }


  /**
   * 对点击流数据进行转换及拉宽操作
   * * 1：将点击流字符串数据转换成点击流对象
   * * 2：对点击流对象进行实时拉宽,生成点击流宽对象,并返回
   *
   * @param clicklogDataStream
   * @return
   */
  def etl(clicklogDataStream: DataStream[String]): DataStream[ClickLogWideEntity] = {

    //1.将点击流数据转换成点击流对象
    val clickLogEntityDataStream: DataStream[ClickLogEntity] = clicklogDataStream.map(new RichMapFunction[String, ClickLogEntity] {
      //定义日志解析器
      var parser: HttpdLoglineParser[ClickLogEntity] = _;

      /**
       * 创建日志解析器
       */
      override def open(parameters: Configuration): Unit = {
        parser = ClickLogEntity.createClickLogParser()
      }

      /**
       * 对点击流数据一条条的转换成对象
       */
      override def map(line: String): ClickLogEntity = {
        ClickLogEntity(line, parser)
      }
    })


    //2.将点击流对象进行拉宽操作转换成点击流宽对象
    val clickLogWideEntityDataStream: DataStream[ClickLogWideEntity] = clickLogEntityDataStream.map(new RichMapFunction[ClickLogEntity, ClickLogWideEntity] {

      //定义ip获取到的省份城市的实例对象
      var iPSeeker: IPSeeker = _;

      /**
       * 加载分布式缓存文件
       */
      override def open(parameters: Configuration): Unit = {
        //读取分布式缓存文件
        val ipDataFile: File = getRuntimeContext.getDistributedCache.getFile("qqwry.dat")

        //利用IP工具类初始化IPseeker实例
        iPSeeker = new IPSeeker(ipDataFile)
      }


      /**
       * 对数据进行一条条的处理
       */
      override def map(clickLogEntity: ClickLogEntity): ClickLogWideEntity = {
        //将点击流日志转换成拉宽后的点击流对象中
        val clickLogWideEntity: ClickLogWideEntity = ClickLogWideEntity(clickLogEntity)


        //** 根据IP解析"省,市,区"信息,然后存放到点击流宽对象的字段中
        val conuntry = iPSeeker.getCountry(clickLogWideEntity.getIp)
        var areaArray = conuntry.split("省")
        if (areaArray.size > 1) {
          //表示非直辖市
          clickLogWideEntity.province = areaArray(0) + "省"
          val regionArray = areaArray(1).split("市")
          if (regionArray.size > 1) {
            clickLogWideEntity.city = regionArray(0) + "市"
          } else {
            clickLogWideEntity.city = regionArray(0)
          }
        } else {
          //表示直辖市
          areaArray = conuntry.split("市")
          if (areaArray.size > 1) {
            clickLogWideEntity.province = areaArray(0) + "市"
            clickLogWideEntity.city = areaArray(1)
          } else {
            clickLogWideEntity.province = areaArray(0)
            clickLogWideEntity.city = areaArray(0)
          }
        }

        try {
          //获取访问的时间
          val requestTime: String = clickLogWideEntity.requestTime
          //将字符串类型转换成时间戳类型
          //05/Sep/2010:11:27:50 +0200
          // fixme : mmm是英文月份的前三个字母的缩写
          val dateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss", Locale.ENGLISH)
          val date: Date = dateFormat.parse(requestTime)
          clickLogWideEntity.timestamp = DateFormatUtils.format(date, "yyyy-MM-dd HH:mm:ss")
        } catch {
          case exception: Exception => println(exception)
        }



        //返回拉宽后的点击流对象
        clickLogWideEntity
      }
    })

    clickLogWideEntityDataStream
  }
}

object ClickLogDataETL {
  def main(args: Array[String]): Unit = {
    /*val dateFormat: SimpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss", Locale.ENGLISH)
    val date: Date = dateFormat.parse("05/Sep/2010:11:27:50 +0200")
    println(DateFormatUtils.format(date, "yyyy-MM-dd HH:mm:ss"))*/
  }
}

