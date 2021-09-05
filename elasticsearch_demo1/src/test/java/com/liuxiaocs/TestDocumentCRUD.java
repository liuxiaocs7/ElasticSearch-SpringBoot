package com.liuxiaocs;

import com.alibaba.fastjson.JSONObject;
import com.liuxiaocs.entity.Book;
import com.liuxiaocs.entity.Emp;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class TestDocumentCRUD {

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

    // 添加一个文档  指定id
    @Test
    public void testCreate() {
        // name age bir content address
        Book book = new Book("1", "我们2001的故事", 23, new Date(), "我们在疫情下努力学习！", "北京");

        // 将对象转为json
        String json = JSONObject.toJSONString(book);

        // 索引为dangdang，类型为book，id为1的数据
        // 索引一条文档
        // 将数据转为json字符串格式，并显式指定类型为JSON
        // 返回一个索引响应对象
        IndexResponse indexResponse = transportClient.prepareIndex("dangdang", "book", book.getId()).setSource(json, XContentType.JSON).get();
        // 返回一个操作状态  CREATED
        System.out.println(indexResponse.status());
    }

    // 添加一个文档  自动id
    @Test
    public void testCreateAutoId() {
        // name age bir content address
        Book book = new Book(null, "我们2001的故事1", 23, new Date(), "我们在疫情下努力学习！好好听课", "北京");

        // 将对象转为json
        String json = JSONObject.toJSONString(book);

        // 索引为dangdang，类型为book，id为1的数据
        // 索引一条文档
        // 将数据转为json字符串格式，并显式指定类型为JSON
        // 返回一个索引响应对象
        IndexResponse indexResponse = transportClient.prepareIndex("dangdang", "book").setSource(json, XContentType.JSON).get();
        // 返回一个操作状态  CREATED
        System.out.println(indexResponse.status());
    }

    // 更新一条文档
    @Test
    public void testUpdate() {
        // Book book = new Book(null, "我们2001马上结束框架学习", 23, new Date(), "我们即将结束框架学习，我们非常不舍陈老师，小陈老师特别好", "北京");
        Book book = new Book();
        book.setName("我们2001马上结束框架学习").setContent("我们即将结束框架学习，我们非常不舍陈老师，小陈老师特别好").setBir(new Date());
        String json = JSONObject.toJSONStringWithDateFormat(book, "yyyy-MM-dd");
        // 更新一个文档  保留原始数据更新
        UpdateResponse updateResponse = transportClient.prepareUpdate("dangdang", "book", "1").setDoc(json, XContentType.JSON).get();
        System.out.println(updateResponse.status());
    }

    // 删除一条文档
    @Test
    public void testDelete() {
        // 传入文档索引即可
        DeleteResponse deleteResponse = transportClient.prepareDelete("dangdang", "book", "5tzeoXsB53o8ewiOZnxg").get();
        System.out.println(deleteResponse);
    }

    // 查询一条
    @Test
    public void testFindOne() throws ParseException {
        // 获取返回数据作为一个字符串
        GetResponse getResponse = transportClient.prepareGet("dangdang", "book", "1").get();
        System.out.println(getResponse.getSourceAsString());

        // 获取返回数据作为一个Map
        Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
        Book book = new Book();
        book.setId(sourceAsMap.get("id").toString());
        // 使用yyyy-MM-dd格式来解析数据
        book.setBir(new SimpleDateFormat("yyyy-MM-dd").parse(sourceAsMap.get("bir").toString()));
        System.out.println(book);
    }

    // 各种查询  查询所有
    @Test
    public void testSearch() {
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        SearchResponse searchResponse = transportClient.prepareSearch("dangdang")  // 指定索引
                .setTypes("book")  // 指定类型
                .setQuery(matchAllQueryBuilder)    // 指定查询条件
                .get();      // 执行查询
        System.out.println("总条数：" + searchResponse.getHits().getTotalHits());
        System.out.println("最大得分：" + searchResponse.getHits().getMaxScore());
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    // termQuery
    @Test
    public void testQuery() {
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("content", "学习");
        SearchResponse searchResponse = transportClient.prepareSearch("dangdang")
                .setTypes("book")
                .setQuery(termQueryBuilder)   // 查询条件
                .get();
        System.out.println("符合条件的记录数：" + searchResponse.getHits().getTotalHits());
        System.out.println("符合条件的记录数：" + searchResponse.getHits().getMaxScore());
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    // 批量操作
    @Test
    public void testBulk() {
        Emp emp = new Emp("12", "张三丰", 23, new Date(), "太极创始人", "武当山");
        // 添加
        IndexRequest indexRequest = new IndexRequest("ems", "emp", emp.getId()).source(JSONObject.toJSONStringWithDateFormat(emp, "yyyy-MM-dd"), XContentType.JSON);
        // 删除
        DeleteRequest deleteRequest = new DeleteRequest("ems", "emp", "6dz7oXsB53o8ewiOCnzX");
        // 修改
        Emp emp1 = new Emp();
        emp1.setContent("Spring 框架是一个分层架构，由 7 个定义良好的模块组成，这是要给开源框架，它非常不错！使用真的很方便");
        UpdateRequest updateRequest = new UpdateRequest("ems", "emp", "6Nz7oXsB53o8ewiOCnzX").doc(JSONObject.toJSONString(emp1), XContentType.JSON);

        // 返回批量更新对象
        BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
        BulkResponse bulkItemResponses = bulkRequestBuilder
                .add(indexRequest)
                .add(deleteRequest)
                .add(updateRequest)
                .get();
        // 数组中的每一个值表示一个执行的响应
        BulkItemResponse[] items = bulkItemResponses.getItems();
        for (BulkItemResponse item : items) {
            System.out.println(item.status());
        }
    }
}
