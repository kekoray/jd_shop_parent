package com.jd.canal_client.bean;

import com.alibaba.fastjson.JSON;
import com.google.protobuf.InvalidProtocolBufferException;
import com.jd.canal_client.protobuf.CanalModel;
import com.jd.canal_client.protobuf.ProtoBufable;

import java.util.HashMap;
import java.util.Map;

/**
 * @ProjectName: jd_shop_parent
 * @program: com.jd.canal_client
 * @FileName: CanalRowData
 * @description: 将map对象中的数据解析出来赋值给CanalModel对象
 * @version: 1.0
 * *
 * @author: koray
 * @create: 2020-08-27 20:56
 * @Copyright (c) 2020,All Rights Reserved.
 */
public class CanalRowData implements ProtoBufable {

    //定义变量
    private String logfileName;
    private Long logfileOffset;
    private Long executeTime;
    private String schemaName;
    private String tableName;
    private String eventType;
    private Map<String, String> columns;


    /**
     * 定义构造方法，获得map对象中的binlog日志数据
     *
     * @param map
     */
    public CanalRowData(Map map) {
        this.logfileName = map.get("logfileName").toString();
        this.logfileOffset = Long.parseLong(map.get("logfileOffset").toString());
        this.executeTime = Long.parseLong(map.get("executeTime").toString());
        this.schemaName = map.get("schemaName").toString();
        this.tableName = map.get("tableName").toString();
        this.eventType = map.get("eventType").toString();
        this.columns = (Map<String, String>) map.get("columns");
    }



    /**
     * 序列化 (对象 ==> byte[])
     * 将binglog日志转换成的protobuf格式数据,并返回序列化后的字节数组
     *
     * @return
     */
    @Override
    public byte[] toBytes() {

        CanalModel.RowData.Builder builder = CanalModel.RowData.newBuilder();
        builder.setLogfileName(this.logfileName);
        builder.setLogfileOffset(this.logfileOffset);
        builder.setExecuteTime(this.executeTime);
        builder.setSchemaName(this.schemaName);
        builder.setTableName(this.tableName);
        builder.setEventType(this.eventType);
        for (String key :  this.getColumns().keySet()) {
            builder.putColumns(key, this.getColumns().get(key));
        }

        //将传递的binlog日志解析后,并序列化成字节码数据将其返回
        return builder.build().toByteArray();
    }


    /**
     * 反序列化 (byte[] ==> 对象)
     * 传递一个字节码的数组，将字节码数据反序列化成对象返回
     *
     * @param bytes
     */
    public CanalRowData(byte[] bytes) {
        try {
            //将字节码数据反序列化成对象
            CanalModel.RowData rowData = CanalModel.RowData.parseFrom(bytes);
            this.logfileName = rowData.getLogfileName();
            this.logfileOffset = rowData.getLogfileOffset();
            this.executeTime = rowData.getExecuteTime();
            this.schemaName = rowData.getSchemaName();
            this.tableName = rowData.getTableName();
            this.eventType = rowData.getEventType();

            //将所有的列的集合添加到map集合中
            this.columns = new HashMap();
            //getColumns ==同等与==> getColumnsMap
            this.columns.putAll(rowData.getColumnsMap());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }


    public String getLogfileName() {
        return logfileName;
    }

    public void setLogfileName(String logfileName) {
        this.logfileName = logfileName;
    }

    public Long getLogfileOffset() {
        return logfileOffset;
    }

    public void setLogfileOffset(Long logfileOffset) {
        this.logfileOffset = logfileOffset;
    }

    public Long getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(Long executeTime) {
        this.executeTime = executeTime;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Map<String, String> getColumns() {
        return columns;
    }

    public void setColumns(Map<String, String> columns) {
        this.columns = columns;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
