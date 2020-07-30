package com.natural.data.analyze.elastic;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ClientManager {

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.56.103", 9200, "http")
                )
        );

        /**
         *   index  =>  mysql 中的一个 库
         *
         *   type   => mysql 的 table
         *
         *   document    => mysql  一行数据
         *
         *   field    =>  mysql 的列，一个属性
         *

         */

        //  create index
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("facebook");
        createIndexRequest.settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1)
        );

        Map<String, Object> message = new HashMap<>();
        message.put("type", "text");
        Map<String, Object> properties = new HashMap<>();
        properties.put("message", message);
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", properties);
        createIndexRequest.mapping(mapping);

        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);

//        createIndexRequest.source("", XContentType.JSON);

        System.out.println(createIndexResponse.isAcknowledged());

        // get index    curl -i -X GET http://192.168.56.103:9200/facebook


        //  curl -i -X PUT  http://192.168.56.103:9200/facebook/_doc/1   添加内容





        client.close();

    }
}
