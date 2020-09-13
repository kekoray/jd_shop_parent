package cn.jd.shop.realtime.etl.bean

import com.jd.canal_client.bean.CanalRowData

import scala.beans.BeanProperty

/**
 *
 * @ProjectName: jd_shop_parent  
 * @program: cn.jd.shop.realtime.etl
 * @FileName: OrderDBEntity
 * @description: 订单表样例类
 * @version: 1.0
 *           *
 * @author: koray
 * @create: 2020-08-30 21:43
 * @Copyright (c) 2020,All Rights Reserved.
 */
case class OrderDBEntity(@BeanProperty orderId: Long, //订单id
                         @BeanProperty orderNo: String, //订单编号
                         @BeanProperty userId: Long, //用户id
                         @BeanProperty orderStatus: Int, //订单状态 -3:用户拒收-2:未付款的订单-1：用户取消 0:待发货 1:配送中 2:用户确认收货
                         @BeanProperty goodsMoney: Double, //商品金额
                         @BeanProperty deliverType: Int, //收货方式0:送货上门1:自提
                         @BeanProperty deliverMoney: Double, //运费
                         @BeanProperty totalMoney: Double, //订单金额（包括运费）
                         @BeanProperty realTotalMoney: Double, //实际订单金额（折扣后金额）
                         @BeanProperty payType: Int, //支付方式
                         @BeanProperty isPay: Int, //是否支付0:未支付1:已支付
                         @BeanProperty areaId: Int, //区域最低一级
                         @BeanProperty areaIdPath: String, //区域idpath
                         @BeanProperty userName: String, //收件人姓名
                         @BeanProperty userAddress: String, //收件人地址
                         @BeanProperty userPhone: String, //收件人电话
                         @BeanProperty orderScore: Int, //订单所得积分
                         @BeanProperty isInvoice: Int, //是否开发票1:需要0:不需要
                         @BeanProperty invoiceClient: String, //发票抬头
                         @BeanProperty orderRemarks: String, //订单备注
                         @BeanProperty orderSrc: Int, //订单来源0:商城1:微信2:手机版3:安卓App4:苹果App
                         @BeanProperty needPay: Double, //需缴费用
                         @BeanProperty payRand: Int, //货币单位
                         @BeanProperty orderType: Int, //订单类型
                         @BeanProperty isRefund: Int, //是否退款0:否1：是
                         @BeanProperty isAppraise: Int, //是否点评0:未点评1:已点评
                         @BeanProperty cancelReason: Int, //取消原因ID
                         @BeanProperty rejectReason: Int, //用户拒绝原因ID
                         @BeanProperty rejectOtherReason: String, //用户拒绝其他原因
                         @BeanProperty isClosed: Int, //订单是否关闭
                         @BeanProperty goodsSearchKeys: String,
                         @BeanProperty orderunique: String, //订单流水号
                         @BeanProperty receiveTime: String, //收货时间
                         @BeanProperty deliveryTime: String, //发货时间
                         @BeanProperty tradeNo: String, //在线支付交易流水
                         @BeanProperty dataFlag: Int, //订单有效标志 -1：删除 1:有效
                         @BeanProperty createTime: String, //下单时间
                         @BeanProperty settlementId: Int, //是否结算，大于0的话则是结算ID
                         @BeanProperty commissionFee: Double, //订单应收佣金
                         @BeanProperty scoreMoney: Double, //积分抵扣金额
                         @BeanProperty useScore: Int, //花费积分
                         @BeanProperty orderCode: String,
                         @BeanProperty extraJson: String, //额外信息
                         @BeanProperty orderCodeTargetId: Int,
                         @BeanProperty noticeDeliver: Int, //提醒发货 0:未提醒 1:已提醒
                         @BeanProperty invoiceJson: String, //发票信息
                         @BeanProperty lockCashMoney: Double, //锁定提现金额
                         @BeanProperty payTime: String, //支付时间
                         @BeanProperty isBatch: Int, //是否拼单
                         @BeanProperty totalPayFee: Int, //总支付金额
                         @BeanProperty isFromCart: Int //是否来自购物车 0：直接下单  1：购物车
                        )

/**
 * 订单表的伴生对象
 */
object OrderDBEntity {
  //将从kafka消费出来的数据反序列化后的CanalRowData对象，转换成订单的样例类
  def apply(canalRowData: CanalRowData): OrderDBEntity = {
    new OrderDBEntity(
      canalRowData.getColumns.get("orderId").toInt,
      canalRowData.getColumns.get("orderNo"),
      canalRowData.getColumns.get("userId").toLong,
      canalRowData.getColumns.get("orderStatus").toInt,
      canalRowData.getColumns.get("goodsMoney").toDouble,
      canalRowData.getColumns.get("deliverType").toInt,
      canalRowData.getColumns.get("deliverMoney").toDouble,
      canalRowData.getColumns.get("totalMoney").toDouble,
      canalRowData.getColumns.get("realTotalMoney").toDouble,
      canalRowData.getColumns.get("payType").toInt,
      canalRowData.getColumns.get("isPay").toInt,
      canalRowData.getColumns.get("areaId").toInt,
      canalRowData.getColumns.get("areaIdPath"),
      canalRowData.getColumns.get("userName"),
      canalRowData.getColumns.get("userAddress"),
      canalRowData.getColumns.get("userPhone"),
      canalRowData.getColumns.get("orderScore").toInt,
      canalRowData.getColumns.get("isInvoice").toInt,
      canalRowData.getColumns.get("invoiceClient"),
      canalRowData.getColumns.get("orderRemarks"),
      canalRowData.getColumns.get("orderSrc").toInt,
      canalRowData.getColumns.get("needPay").toDouble,
      canalRowData.getColumns.get("payRand").toInt,
      canalRowData.getColumns.get("orderType").toInt,
      canalRowData.getColumns.get("isRefund").toInt,
      canalRowData.getColumns.get("isAppraise").toInt,
      canalRowData.getColumns.get("cancelReason").toInt,
      canalRowData.getColumns.get("rejectReason").toInt,
      canalRowData.getColumns.get("rejectOtherReason"),
      canalRowData.getColumns.get("isClosed").toInt,
      canalRowData.getColumns.get("goodsSearchKeys"),
      canalRowData.getColumns.get("orderunique"),
      canalRowData.getColumns.get("receiveTime"),
      canalRowData.getColumns.get("deliveryTime"),
      canalRowData.getColumns.get("tradeNo"),
      canalRowData.getColumns.get("dataFlag").toInt,
      canalRowData.getColumns.get("createTime"),
      canalRowData.getColumns.get("settlementId").toInt,
      canalRowData.getColumns.get("commissionFee").toDouble,
      canalRowData.getColumns.get("scoreMoney").toDouble,
      canalRowData.getColumns.get("useScore").toInt,
      canalRowData.getColumns.get("orderCode"),
      canalRowData.getColumns.get("extraJson"),
      canalRowData.getColumns.get("orderCodeTargetId").toInt,
      canalRowData.getColumns.get("noticeDeliver").toInt,
      canalRowData.getColumns.get("invoiceJson"),
      canalRowData.getColumns.get("lockCashMoney").toDouble,
      canalRowData.getColumns.get("payTime"),
      canalRowData.getColumns.get("isBatch").toInt,
      canalRowData.getColumns.get("totalPayFee").toInt,
      canalRowData.getColumns.get("isFromCart").toInt
    )
  }
}
