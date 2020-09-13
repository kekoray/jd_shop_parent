package cn.jd.shop.realtime.etl.bean

import nl.basjes.parse.httpdlog.HttpdLoglineParser

import scala.beans.BeanProperty


/**
 *
 * @ProjectName: jd_shop_parent  
 * @program: cn.jd.shop.realtime.etl.bean   
 * @FileName: ClickLogEntity 
 * @description: 定义点击流日志的样例类,将解析好的日志数据封装到样例类中
 * @version: 1.0   
 *           *
 * @author: koray   
 * @create: 2020-08-29 20:03  
 * @Copyright (c) 2020,All Rights Reserved.
 */
case class ClickLogEntity(
                           /**
                            * @BeanProperty 只能用于开头为字母的字段,且要设置var, 才有set/get方法
                            */
                           @BeanProperty var connectionClientUser: String = "", //用户id信息
                           @BeanProperty var connectionClientHost: String = "", //ip地址
                           @BeanProperty var requestTime: String = "", //请求时间
                           @BeanProperty var method: String = "", //请求方法
                           @BeanProperty var resolution: String = "", //请求资源
                           @BeanProperty var requestProtocol: String = "", //请求协议
                           @BeanProperty var requestStatus: String = "", //请求状态
                           @BeanProperty var responseBodyBytes: String = "", //请求流量
                           @BeanProperty var referer: String = "", //访问来源URL
                           @BeanProperty var userAgent: String = "", //客户端代理信息
                           @BeanProperty var refererDomain: String = "" //跳转页面的域名
                         ) {
  // toString方法
  override def toString: String = s"ClickLogEntity($getConnectionClientUser(),$getConnectionClientHost(),$getRequestTime()," +
    s"$getMethod(),$getResolution(),$getRequestProtocol(),$getRequestStatus(),$getResponseBodyBytes(),$getReferer()," +
    s"$getUserAgent(),$getRefererDomain())"
}

/**
 *
 * @ClassName: ClickLogEntity
 * @description: 伴生对象,接受点击流日志字符串,将其解析转换成点击流对象返回
 * @param: * @param null
 * @return:
 * @author: koray
 * @create: 2020/8/29
 * @since: JDK 1.8
 */
object ClickLogEntity {
  //1.定义点击流日志的解析规则
  val getLogFormat: String = "%u %h %l %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\""

  def apply(clickLog: String, parse: HttpdLoglineParser[ClickLogEntity]): ClickLogEntity = {

    val ClickLogEntity = new ClickLogEntity

    //将解析的数据封装到ClickLogEntity中
    parse.parse(ClickLogEntity, clickLog)

    ClickLogEntity
  }


  /**
   * 创建解析器
   */
  def createClickLogParser(): HttpdLoglineParser[ClickLogEntity] = {
    //创建解析器
    val parser = new HttpdLoglineParser[ClickLogEntity](classOf[ClickLogEntity], getLogFormat)

    //建立对象的方法与参数名的映射关系
    parser.addParseTarget("setConnectionClientUser", "STRING:connection.client.user")
    parser.addParseTarget("setConnectionClientHost", "IP:connection.client.host")
    parser.addParseTarget("setRequestTime", "TIME.STAMP:request.receive.time")
    parser.addParseTarget("setMethod", "HTTP.METHOD:request.firstline.method")
    parser.addParseTarget("setResolution", "HTTP.URI:request.firstline.uri")
    parser.addParseTarget("setRequestProtocol", "HTTP.PROTOCOL_VERSION:request.firstline.protocol")
    parser.addParseTarget("setResponseBodyBytes", "BYTES:response.body.bytes")
    parser.addParseTarget("setReferer", "HTTP.URI:request.referer")
    parser.addParseTarget("setUserAgent", "HTTP.USERAGENT:request.user-agent")
    parser.addParseTarget("setRefererDomain", "HTTP.HOST:request.referer.host")

    parser
  }
}

case class ClickLogWideEntity(
                               @BeanProperty var uid: String, //用户id信息
                               @BeanProperty var ip: String, //ip地址
                               @BeanProperty var requestTime: String, //请求时间
                               @BeanProperty var requestMethod: String, //请求方式
                               @BeanProperty var requestUrl: String, //请求地址
                               @BeanProperty var requestProtocol: String, //请求协议
                               @BeanProperty var responseStatus: String, //响应码
                               @BeanProperty var responseBodyBytes: String, //返回的数据流量
                               @BeanProperty var referrer: String, //访客的来源url
                               @BeanProperty var userAgent: String, //客户端代理信息
                               @BeanProperty var referDomain: String, //跳转过来页面的域名
                               @BeanProperty var province: String, //ip所对应的省份
                               @BeanProperty var city: String, //ip所对应的城市
                               @BeanProperty var timestamp: String //时间戳
                             )


/**
 * 伴生对象
 */
object ClickLogWideEntity {
  /**
   * 传递点击流对象进来，转换成拉宽后的点击流对象
   *
   * @param clickLogEntity
   * @return
   */
  def apply(clickLogEntity: ClickLogEntity): ClickLogWideEntity = {
    new ClickLogWideEntity(
      clickLogEntity.getConnectionClientUser,
      clickLogEntity.getConnectionClientHost,
      clickLogEntity.getRequestTime,
      clickLogEntity.getMethod,
      clickLogEntity.getResolution,
      clickLogEntity.getRequestProtocol,
      clickLogEntity.getRequestStatus,
      clickLogEntity.getResponseBodyBytes,
      clickLogEntity.getReferer,
      clickLogEntity.getUserAgent,
      clickLogEntity.getRefererDomain,
      "",
      "",
      ""
    )
  }
}

