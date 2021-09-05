package com.liuxiaocs;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class TestElasticSearchClient {

    @Test
    public void testInit() throws UnknownHostException {
        // 创建ES客户端对象
        TransportClient transportClient = new PreBuiltTransportClient(Settings.EMPTY);
        // 设置操作ES服务主机和端口
        transportClient.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.200.129"), 9300));

        // 操作

        // 释放
        transportClient.close();

    }
}
