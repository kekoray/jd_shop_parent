package com.jd.canal_client;

import com.jd.canal_client.CanalClient;

/**
 * @ProjectName: jd_shop_parent
 * @program: com.jd.canal_client.util
 * @FileName: Entrance
 * @description: canal客户端的入口程序
 * @version: 1.0
 * *
 * @author: koray
 * @create: 2020-08-27 23:15
 * @Copyright (c) 2020,All Rights Reserved.
 */
public class Entrance {
    public static void main(String[] args) {
        CanalClient client = new CanalClient();
        client.start();
    }
}
