package cn.jd.shop.realtime.etl.app

import cn.jd.shop.realtime.etl.process.{CartDataETL, ClickLogDataETL, CommentsDataETL, GoodsDataETL, OrderDataETL, OrderDetailDataETL, SyncDimData}
import cn.jd.shop.realtime.etl.utils.GlobalConfigUtil
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 *
 * @ProjectName: jd_shop_parent  
 * @program: cn.jd.shop.realtime.etl.app   
 * @FileName: app 
 * @description: Flink实时数据ETL业务处理核心类
 *               实现步骤：
 *               1：初始化flink的流式计算环境
 *               2：设置flink的并行度为1，测试环境设置为1，生产环境需要注意，尽可能使用client递交作业的时候指定并行度
 *               3：开启flink的checkpoint
 *               4: 接入kafka的数据源,实现所有的ETL业务
 *               4-1）实现维度数据的增量更新
 *               4-2）点击流数据的实时ETL
 *               4-3）订单数据的实时ETL
 *               4-4）订单明细数据的实时ETL
 *               4-5）购物车数据的实时ETL
 *               4-6）评论数据的实时ETL
 *               5: 执行任务
 * @version: 1.0   
 *           *
 * @author: koray   
 * @create: 2020-08-28 16:21  
 * @Copyright (c) 2020,All Rights Reserved.
 */
object app {
  def main(args: Array[String]): Unit = {
    //1：初始化flink的流式计算环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2：设置flink的并行度为1，测试环境设置为1，生产环境需要注意，尽可能使用client递交作业的时候指定并行度
    env.setParallelism(1)

    //3：开启flink的checkpoint
    //开启checkpoint的时候，设置checkpoint的运行周期，每隔5秒钟进行一次chckpoint
    env.enableCheckpointing(5000)
    //当作业被cancel的时候，保留以前的checkpoint，为了避免数据的丢失
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //设置同一时间只能有一个检查点，检查点的操作是否可以并行，1不能并行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    /**
     * checkpoint的保存位置
     * 目前版本使用的话会报异常,故可以不用
     * java.util.ServiceConfigurationError:
     * org.apache.hadoop.fs.FileSystem: Provider org.apache.hadoop.fs.s3native.NativeS3FileSystem could not be instantiated
     */
    env.setStateBackend(new FsStateBackend("hdfs://node-1:8020/flink/checkpoint/"))

    //配置两次checkpoint的最小时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    //配置checkpoint的超时时长
    env.getCheckpointConfig.setCheckpointTimeout(60000)

    //指定重启策略，默认的情况下是不停的重启
    //程序出现异常的时候，会进行重启，重启五次，每次延迟5秒钟，如果超过了五次，则程序退出
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 5000))

    /**
     * 使用分布式缓存将IP地址资源库信息拷贝到task manager节点上
     * *生产环境下，建议将ip地址资源库信息维护到关系型数据库中，
     * *因为资源库信息有可能会发生变化(新的ip地址段推出),可以使用flink的广播流解决资源库信息变更的问题
     */
    env.registerCachedFile(GlobalConfigUtil.`ip.file.path`, "qqwry.dat")


    //4：接入kafka的数据源,实现所有的ETL业务
    //* 1）实现维度数据的实时增量更新
    val syncDimData: SyncDimData = new SyncDimData(env)
    syncDimData.process()


    //* 2）点击流数据的实时ETL
    val clickLogData: ClickLogDataETL = new ClickLogDataETL(env)
    clickLogData.process()


    //* 3）订单数据的实时ETL
    val orderData: OrderDataETL = new OrderDataETL(env)
    orderData.process()


    // * 4）订单明细数据的实时ETL
    val orderDetailDataProcess = new OrderDetailDataETL(env)
    orderDetailDataProcess.process()


    //* 商品数据的实时ETL
    val goodsDataProcess = new GoodsDataETL(env)
    goodsDataProcess.process()


    //* 5）购物车数据的实时ETL
    val cartDataProcess: CartDataETL = new CartDataETL(env)
    cartDataProcess.process()


    //* 6）评论数据的实时ETL
    val commentsDataProcess = new CommentsDataETL(env)
    commentsDataProcess.process()


    //5：执行任务
    env.execute(this.getClass.getSimpleName);
  }
}
