package com.jd.canal_client.protobuf;


import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * @ProjectName: jd_shop_parent
 * @program: com.jd.canal_client.protobuf
 * @FileName: ProtoBufSerializer
 * @description: 因为binlog日志解析成protobuf的数据结构写入到Kafka中,
 *               故Kafka要有个能解析protobuf格式对应的序列化机制去读取传入的数据.
 * @version: 1.0
 * *
 * @author: koray
 * @create: 2020-08-27 22:18
 * @Copyright (c) 2020,All Rights Reserved.
 */
public class ProtoBufSerializer implements Serializer<ProtoBufable> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, ProtoBufable data) {
        return data.toBytes();
    }

    @Override
    public void close() {

    }
}
