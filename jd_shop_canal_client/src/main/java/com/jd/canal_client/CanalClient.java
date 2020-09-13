package com.jd.canal_client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.protobuf.InvalidProtocolBufferException;
import com.jd.canal_client.bean.CanalRowData;
import com.jd.canal_client.kafka.KafkaSender;
import com.jd.canal_client.util.ConfigUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ProjectName: jd_shop_parent
 * @program: com.jd.canal_client
 * @FileName: CanalClient
 * @description: CanalClient核心处理类
 * @version: 1.0
 * *
 * @author: koray
 * @create: 2020-08-27 20:04
 * @Copyright (c) 2020,All Rights Reserved.
 */
public class CanalClient {
    //定义canal的客户端连接器
    private CanalConnector canalConnector = null;
    //定义kafka的生产者实例
    private KafkaSender kafkaSender = null;
    //一次拉取的binlog数据的条数
    private int BATCH_SIZE = 5 * 1024;
    private boolean isRunning = true;


    /**
     * 构造方法
     */
    public CanalClient() {
        //初始化客户端连接器
        canalConnector = CanalConnectors.newClusterConnector(ConfigUtil.zookeeperServerIp(),
                ConfigUtil.canalserverdestination(),
                ConfigUtil.canalserverusername(),
                ConfigUtil.canalserverpassword());

        //实例化Kafka生产者对象
        kafkaSender = new KafkaSender();
    }


    /**
     * 开始执行
     */
    public void start() {
        try {
            //建立连接
            canalConnector.connect();
            //回滚上次的get请求,重新获取数据
            canalConnector.rollback();
            canalConnector.subscribe(ConfigUtil.canalSubscribeFilter());

            while (isRunning) {
                //获取binlog日志，每次拉取的数据条数
                Message message = canalConnector.getWithoutAck(BATCH_SIZE);
                //获取batchid
                long batchId = message.getId();
                //获取binlog数据的条数
                int size = message.getEntries().size();

                if (size == 0) {
                    //没有数据
                } else {  //获取到数据进行解析操作

                    //将binlog日志进行解析，解析后的数据就是map对象
                    Map binglogMessageToMap = binglogMessageToMap(message);

                    if (binglogMessageToMap.size() > 0) {
                        //将map对象转换成protobuf数据, 有可能会报空值指针异常,表示数据有可能会有异常数据
                        CanalRowData canalRowData = new CanalRowData(binglogMessageToMap);
                        //数据打印
                        System.out.println(canalRowData);

                        //有数据，将数据发送到kafka集群
                        kafkaSender.send(canalRowData);
                    }


                }

                canalConnector.ack(batchId);
            }
        } catch (CanalClientException e) {
            e.printStackTrace();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        } finally {
            //关闭连接
            canalConnector.disconnect();
        }
    }

    private Map binglogMessageToMap(Message message) throws InvalidProtocolBufferException {
        HashMap rowDataMap = new HashMap();

        // 1. 遍历message中的所有binlog实体
        for (CanalEntry.Entry entry : message.getEntries()) {

            // 只处理事务型binlog
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN ||
                    entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            // 获取binlog文件名
            String logfileName = entry.getHeader().getLogfileName();
            // 获取logfile的偏移量
            long logfileOffset = entry.getHeader().getLogfileOffset();
            // 获取sql语句执行时间戳
            long executeTime = entry.getHeader().getExecuteTime();
            // 获取数据库名
            String schemaName = entry.getHeader().getSchemaName();
            // 获取表名
            String tableName = entry.getHeader().getTableName();
            // 获取事件类型 insert/update/delete
            String eventType = entry.getHeader().getEventType().toString().toLowerCase();

            rowDataMap.put("logfileName", logfileName);
            rowDataMap.put("logfileOffset", logfileOffset);
            rowDataMap.put("executeTime", executeTime);
            rowDataMap.put("schemaName", schemaName);
            rowDataMap.put("tableName", tableName);
            rowDataMap.put("eventType", eventType);

            //获得所有行的变更数据
            HashMap<String, String> columnDataMap = new HashMap<>();
            //getStoreValue  ==>  获得二进制数组
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
            for (CanalEntry.RowData rowData : rowDatasList) {
                if (eventType.equals("insert") || eventType.equals("update")) {
                    for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                        columnDataMap.put(column.getName(), column.getValue());
                    }
                } else if (eventType.equals("delete")) {
                    for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                        columnDataMap.put(column.getName(), column.getValue());
                    }
                }
            }
            rowDataMap.put("columns", columnDataMap);
        }

        return rowDataMap;
    }
}
