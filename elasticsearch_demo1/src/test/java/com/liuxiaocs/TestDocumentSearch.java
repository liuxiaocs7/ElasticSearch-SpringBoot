package com.liuxiaocs;

import org.apache.lucene.search.TermQuery;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class TestDocumentSearch {

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

    /**
     * 各种查询
     * 注意使用自定义排序addSort时，计算出来的分数结果时NaN，因为相当于干预了之前的算分逻辑，之前的算分逻辑失效了
     */
    @Test
    public void testQuery() {
        // 查询所有
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();

        // termQuery
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("content", "太极");

        // rangeQuery
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age").gte(0).lte(10);

        // wildcardQuery 通配符 ?:一个 *: 0到多个
        // WildcardQueryBuilder wildcardQueryBuilder = QueryBuilders.wildcardQuery("content", "框?");
        // WildcardQueryBuilder wildcardQueryBuilder = QueryBuilders.wildcardQuery("content", "mvc");
        WildcardQueryBuilder wildcardQueryBuilder = QueryBuilders.wildcardQuery("content", "m*");

        // prefixQuery 前缀查询
        PrefixQueryBuilder prefixQueryBuilder = QueryBuilders.prefixQuery("content", "框");

        // ids 查询
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery().addIds("12").addIds("69z7oXsB53o8ewiOCnzX").addIds("7Nz7oXsB53o8ewiOCnzX");

        // 调用方法
        // testResult(matchAllQueryBuilder);
        // testResult(termQueryBuilder);
        // testResult(rangeQueryBuilder);
        // testResult(wildcardQueryBuilder);
        // testResult(prefixQueryBuilder);
        testResult(idsQueryBuilder);
    }

    // 用来输出搜索结果
    public void testResult(QueryBuilder queryBuilder) {
        SearchResponse searchResponse = transportClient.prepareSearch("ems")
                .setTypes("emp")
                .setQuery(queryBuilder)
                .setFrom(0)  // 起始是第几条，默认从0开始 (当前页 - 1) * size
                .setSize(20)  // 设置每页展示记录数
                .addSort("age", SortOrder.DESC)  // 设置排序 DESC降序 ASC升序
                .get();
        // 获取searchResponse中的hits
        System.out.println("查询符合条件的总条数：" + searchResponse.getHits().totalHits);
        System.out.println("查询符合条件文档的最大得分：" + searchResponse.getHits().getMaxScore());
        // 获取每一个文档的详细信息
        SearchHit[] hits = searchResponse.getHits().getHits();
        for (SearchHit hit : hits) {
            System.out.println("======>: " + hit.getSourceAsString());
        }
    }
}
