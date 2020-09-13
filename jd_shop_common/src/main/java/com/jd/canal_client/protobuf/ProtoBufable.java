package com.jd.canal_client.protobuf;

/**
 * @ProjectName: jd_shop_parent
 * @program: com.jd.canal_client.protobuf
 * @FileName: ProtoBufable
 * @description: 定义一个接口，这个接口中定义toBytes序列化方法
 * @version: 1.0
 * *
 * @author: koray
 * @create: 2020-08-27 21:31
 * @Copyright (c) 2020,All Rights Reserved.
 */
public interface ProtoBufable {

    /**
     * 将对象转换成字节码数组
     */
    byte[] toBytes();
}
