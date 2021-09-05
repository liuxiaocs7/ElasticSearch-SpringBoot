package com.liuxiaocs;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

public class TestIndexAndTypeMapping {

    private TransportClient transportClient;

    @Before
    public void before() throws UnknownHostException {
        // 创建客户端
        this.transportClient = new PreBuiltTransportClient(Settings.EMPTY);
        // 设置es服务地址
        transportClient.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.200.129"), 9300));
    }

    @After
    public void after() {
        transportClient.close();
    }

    // 创建索引
    @Test
    public void testCreateIndex() {
        // admin: 表示管理
        // 创建一个名为dangdang的索引，保证创建的索引不存在
        // 返回一个创建索引的响应对象
        CreateIndexResponse createIndexResponse = transportClient.admin().indices().prepareCreate("dangdang").get();
        // 获取信息
        boolean acknowledged = createIndexResponse.isAcknowledged();
        System.out.println(acknowledged);
    }

    // 删除索引
    @Test
    public void testDeleteIndex() {
        // 删除索引
        AcknowledgedResponse acknowledgedResponse = transportClient.admin().indices().prepareDelete("dangdang").get();
        System.out.println(acknowledgedResponse.isAcknowledged());
    }

    // [了解]  创建索引，创建类型，创建mapping
    @Test
    public void testCreateIndexAndTypeMapping() throws ExecutionException, InterruptedException {
        // 创建一个索引请求对象，操作dangdang这个索引
        CreateIndexRequest dangdangIndex = new CreateIndexRequest("dangdang");
        // 索引设置类型
        // 参数1：类型名  参数2：映射的json数据   参数3:映射格式类型
        dangdangIndex.mapping("book", "{\"properties\":{\"name\":{\"type\":\"text\",\"analyzer\":\"ik_max_word\",\"search_analyzer\":\"ik_max_word\"},\"age\":{\"type\":\"integer\"},\"bir\":{\"type\":\"date\"},\"content\":{\"type\":\"text\",\"analyzer\":\"ik_max_word\",\"search_analyzer\":\"ik_max_word\"},\"address\":{\"type\":\"keyword\"}}}", XContentType.JSON);
        // 创建索引
        CreateIndexResponse createIndexResponse = transportClient.admin().indices().create(dangdangIndex).get();
        System.out.println(createIndexResponse.isAcknowledged());
    }
}
