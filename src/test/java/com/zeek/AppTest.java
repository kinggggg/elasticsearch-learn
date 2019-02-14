package com.zeek;

import static org.junit.Assert.assertTrue;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

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

    //从es中添加或者删除
    @Test
    public void testSaveOrDel() throws Exception {

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
