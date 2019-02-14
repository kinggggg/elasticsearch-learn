package com.zeek;

import static org.junit.Assert.assertTrue;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        assertTrue( true );
    }

    //terms查询
    @Test
    public void testTerms() throws Exception {
        //指定ES集群
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问ES服务的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.56.200"), 9300));

        QueryBuilder builder = QueryBuilders.termsQuery("interests", "changge", "lvyou");

        SearchResponse response = client.prepareSearch("index01")
                .setQuery(builder)
                .setSize(2)
                .get();

        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());

            Map<String, Object> map = hit.getSourceAsMap();
            for (String key : map.keySet()) {
                System.out.println(key + "=" + map.get(key));
            }
        }
    }

    //term查询
    @Test
    public void testTerm() throws Exception {
        //指定ES集群
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问ES服务的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.56.200"), 9300));

        QueryBuilder builder = QueryBuilders.termQuery("interests", "changge");

        SearchResponse response = client.prepareSearch("index01")
                .setQuery(builder)
                .setSize(2)
                .get();

        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());

            Map<String, Object> map = hit.getSourceAsMap();
            for (String key : map.keySet()) {
                System.out.println(key + "=" + map.get(key));
            }
        }
    }

    //multi match查询
    @Test
    public void testMultiMatch() throws Exception {
        //指定ES集群
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问ES服务的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.56.200"), 9300));

        QueryBuilder builder = QueryBuilders.multiMatchQuery("changge", "interests", "address"); //与match不同的是这里第一个参数为要查询的值，字段1，字段2...

        SearchResponse response = client.prepareSearch("index01")
                .setQuery(builder)
                .setSize(3)
                .get();

        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());

            Map<String, Object> map = hit.getSourceAsMap();
            for (String key : map.keySet()) {
                System.out.println(key + "=" + map.get(key));
            }
        }
    }

    //match查询
    @Test
    public void testMatch() throws Exception {
        //指定ES集群
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问ES服务的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.56.200"), 9300));

        QueryBuilder builder = QueryBuilders.matchQuery("interests", "changge"); // 字段,要查询的字段值

        SearchResponse response = client.prepareSearch("index01")
                .setQuery(builder)
                .setSize(3)
                .get();

        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());

            Map<String, Object> map = hit.getSourceAsMap();
            for (String key : map.keySet()) {
                System.out.println(key + "=" + map.get(key));
            }
        }
    }

    //查询所有文档
    @Test
    public void testSearchAll() throws Exception {
        //指定ES集群
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问ES服务的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.56.200"), 9300));

        QueryBuilder qb = QueryBuilders.matchAllQuery();

        SearchResponse sr = client.prepareSearch("index01")
                .setQuery(qb)
                .setSize(3) //虽然是查询所有，但是也可以指定要查询的数量
                .get();

        SearchHits hits = sr.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());

            Map<String, Object> map = hit.getSourceAsMap();
            for (String key : map.keySet()) {
                System.out.println(key + "=" + map.get(key));
            }
        }
    }

    //将查询出的文档进行删除
    @Test
    public void testSearchDel() throws Exception {

        //指定ES集群
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问ES服务的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.56.200"), 9300));

        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE
                .newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("title", "工厂"))
                .source("index01")
                .get();

        long count = response.getDeleted();
        System.out.println("删除了" + count);
    }

    //bulk批量操作
    @Test
    public void testBulk() throws Exception {

        //指定ES集群
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问ES服务的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.56.200"), 9300));

        BulkRequestBuilder bulkRequest = client.prepareBulk();

        bulkRequest.add(client.prepareIndex("lib2", "books", "4")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("title", "python")
                        .field("price", 68)
                        .endObject()
                )
        );
        bulkRequest.add(client.prepareIndex("lib2", "books", "5")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("title", "VR")
                        .field("price", 38)
                        .endObject()
                )
        );
        //批量执行
        BulkResponse bulkResponse = bulkRequest.get();

        System.out.println(bulkResponse.status());
        if (bulkResponse.hasFailures()) {

            System.out.println("存在失败操作");
        }

    }

    //mget批量查询
    @Test
    public void testMget() throws Exception {

        //指定ES集群
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问ES服务的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.56.200"), 9300));

        MultiGetResponse mgResponse = client.prepareMultiGet()
                .add("index1","blog","3","2")
                .add("lib3","user","1","2","3")
                .get();

        for(MultiGetItemResponse response:mgResponse){
            GetResponse rp=response.getResponse();
            if(rp!=null && rp.isExists()){
                System.out.println(rp.getSourceAsString());
            }
        }

    }

    //upsert：存在更新，不存在插入
    @Test
    public void testUpsert() throws Exception {

        //指定ES集群
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问ES服务的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.56.200"), 9300));

        //添加文档
        IndexRequest request1 = new IndexRequest("index01", "blog", "8")
                .source(
                        XContentFactory.jsonBuilder().startObject()
                                .field("id", "2")
                                .field("title", "工场模式")
                                .field("content", "静态工场，实例工场。")
                                .field("postdate", "2018-05-20")
                                .field("url", "csdn.net/79239072")
                                .endObject()
                );

        //若存在id8的文档则进行更新，若不存在执行插入操作
        UpdateRequest request2 = new UpdateRequest("index01", "blog", "8")
                .doc(
                        XContentFactory.jsonBuilder().startObject()
                        .field("title", "设计模式")
                        .endObject()
                ).upsert(request1);

        UpdateResponse response = client.update(request2).get();
        System.out.println(response.status());

    }

    //从es中更新
    @Test
    public void testUpdate() throws Exception {

        //指定ES集群
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问ES服务的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.56.200"), 9300));

        UpdateRequest request = new UpdateRequest();
        request.index("index01")
                .type("blog")
                .id("10")
                .doc(
                        XContentFactory.jsonBuilder().startObject()
                        .field("title", "单例设计模式")
                        .endObject()
                );

        UpdateResponse response = client.update(request).get();
        System.out.println(response.status());

    }

    //从es中添加
    @Test
    public void testDelete() throws Exception {

        //指定ES集群
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问ES服务的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.56.200"), 9300));

        DeleteResponse response = client.prepareDelete("index01", "blog", "10").get();

        System.out.println(response.status());

    }

    //从es中添加
    @Test
    public void testSave() throws Exception {

        //指定ES集群
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问ES服务的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.56.200"), 9300));

        //添加文档
        /**
         * 要添加如下的文档
         * "{" +
         *             "\"id\":\"1\"," +
         *             "\"title\":\"Java设计模式之装饰模式\"," +
         *             "\"content\":\"在不必改变原类文件和使用继承的情况下，动态地扩展一个对象的功能。\"," +
         *             "\"postdate\":\"2018-05-20\"," +
         *             "\"url\":\"csdn.net/79239072\"" +
         *             "}"
         * 首先在es中创建mapping
         * PUT /index01
         * {
         *   "settings": {
         *     "number_of_shards": 3,
         *     "number_of_replicas": 0
         *   },
         *   "mappings": {
         *     "blog":{
         *       "properties":{
         *         "id":{
         *           "type":"long"
         *         },
         *         "title":{
         *           "type":"text",
         *           "analyzer":"ik_max_word"
         *         },
         *         "content":{
         *           "type":"text",
         *           "analyzer":"ik_max_word"
         *         },
         *         "postdate":{
         *           "type":"date"
         *         },
         *         "url":{
         *           "type":"text"
         *         }
         *       }
         *     }
         *   }
         * }
         **/
        XContentBuilder doc = XContentFactory.jsonBuilder()
                .startObject()
                .field("id", "1")
                .field("title", "Java设计模式之装饰模式")
                .field("content", "在不必改变原类文件和使用继承的情况下，动态地扩展一个对象的功能。")
                .field("postdate", "2018-05-20")
                .field("url", "csdn.net/79239072")
                .endObject();

        IndexResponse response = client.prepareIndex("index01", "blog", "10")
                .setSource(doc).get();

        System.out.println(response.status());


    }

    //从es中查询数据
    @Test
    public void testSearch() throws Exception {

        //指定ES集群
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        //创建访问ES服务的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.56.200"), 9300));

        //数据查询
        GetResponse response = client.prepareGet("index1", "type1", "4").execute().actionGet();

        //得到查询出的数据
        System.out.println(response.getSourceAsString());

        client.close();

    }
}
