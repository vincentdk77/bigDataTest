package com.atguigu.kemai.es;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ElasticSearchUtil {
    private static final String HOST_STRING = "node7";
    private static final String CRM_INDEX = "crm";
    private static final String CRM_CUSTOMER_TYPE = "customer";

    public static RestHighLevelClient client;

    public static RestHighLevelClient creatClient() {
        if (client != null) {
            return client;
        }
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(HOST_STRING, 9999, "http")));
        return client;
    }

    public static List<String> postBatchByArray(String tableName, JSONArray array) {
        List<String> resultList = Lists.newArrayList();

        try {
            RestHighLevelClient client = creatClient();
            BulkRequest request = new BulkRequest();

            for (int i = 0; i < array.size(); i++) {
                IndexRequest indexRequest = new IndexRequest(tableName);
                indexRequest.opType(DocWriteRequest.OpType.CREATE);
                indexRequest.source(array.getJSONObject(i), XContentType.JSON);
                request.add(indexRequest);
            }
            BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);

            BulkItemResponse[] responses = response.getItems();
            for (BulkItemResponse bulkItemResponse : responses) {
                resultList.add((bulkItemResponse.getId()));
            }
        } catch (Exception e) {
            System.out.println("--------------------------------------------------" + "表名：" + tableName + " 报错：" + e + "--------------------------------------------------");
            System.out.println("json:" + array.toJSONString());
        }
//        client.close();
        return resultList;
    }

//    public static List<String> postBatchByJSON(String tableName, JSONObject json) {
//        List<String> resultList = Lists.newArrayList();
//
//        try {
//            RestHighLevelClient client = creatClient();
//            BulkRequest request = new BulkRequest();
//
//            IndexRequest indexRequest = new IndexRequest(tableName);
//            indexRequest.opType(DocWriteRequest.OpType.CREATE);
//            indexRequest.source(json, XContentType.JSON);
//            request.add(indexRequest);
//
//            BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
//            BulkItemResponse[] responses = response.getItems();
//
//            for (BulkItemResponse bulkItemResponse : responses) {
//                resultList.add((bulkItemResponse.getId()));
//            }
//            if (resultList.size() == 0) {
//                System.out.println("json:" + json.toJSONString());
//            }
////        client.close();
//        } catch (Exception e) {
//            System.out.println("--------------------------------------------------" + "tableName" + tableName + "--------------------------------------------------");
//            System.out.println("json:" + json.toJSONString());
//            e.printStackTrace();
//        }
//        return resultList;
//    }

    // http://47.92.250.72:9200/posts/doc/1?stored_fields=_none_&_source=false
    // 查看文档是否存在
    public static void isExsit() throws Exception {
        RestHighLevelClient client = creatClient();
        GetRequest getRequest = new GetRequest("posts", "doc", "1");
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        System.out.println(client.exists(getRequest, RequestOptions.DEFAULT));
//        client.close();
    }

    // 获取elastic 文档
    public static void get() throws IOException {
        RestHighLevelClient client = creatClient();
        GetRequest getRequest = new GetRequest(CRM_INDEX);

        // getRequest.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);// 不显示 source字段
        getRequest.realtime(false);// 实时
        getRequest.refresh(true);// 刷新

        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);

        System.out.println(getResponse.getId());
        System.out.println(getResponse.getIndex());
        System.out.println(getResponse.getType());
        if (getResponse.isExists()) {
            long version = getResponse.getVersion();
            String string = getResponse.getSourceAsString();
            Map<String, Object> map = getResponse.getSourceAsMap();
            System.out.println(JSONObject.toJSON(map));
        }
        client.close();
    }

    @SuppressWarnings("deprecation")
    public static JSONObject search(String tableName, JSONObject queryJson) throws Exception {
        String result = "";
        RestHighLevelClient client = creatClient();
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        for (String key : queryJson.keySet()) {
            MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(key, queryJson.getString(key));// 这里可以根据字段进行搜索，must表示符合条件的，相反的mustnot表示不符合条件的
            boolBuilder.must(matchQueryBuilder);
        }

        sourceBuilder.query(boolBuilder); // 设置查询，可以是任何类型的QueryBuilder。
        sourceBuilder.from(0); // 设置确定结果要从哪个索引开始搜索的from选项，默认为0
        sourceBuilder.size(100); // 设置确定搜素命中返回数的size选项，默认为10

        SearchRequest searchRequest = new SearchRequest(tableName); // 索引

        searchRequest.source(sourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits(); // SearchHits提供有关所有匹配的全局信息，例如总命中数或最高分数：
        SearchHit[] searchHits = hits.getHits();
        JSONObject json = new JSONObject();
        for (SearchHit hit : searchHits) {
            result = hit.getSourceAsString();
            json = JSONObject.parseObject(result);
            json.put("_id", hit.getId());
        }
//        client.close();
        return json;
    }

    public static void update(String tableName, String id, Map<String, Object> map) {
        RestHighLevelClient client = creatClient();
        UpdateRequest updateRequest = new UpdateRequest(tableName, id);

        updateRequest.doc(map,XContentType.JSON);
        try {
            client.update(updateRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void updateByField(String tableName, String entId,String field){
        RestHighLevelClient client = creatClient();
        GetRequest getRequest = new GetRequest(tableName,entId);
        try {
            boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
            System.out.println(exists);
            if (exists) {
                UpdateByQueryRequest request = new UpdateByQueryRequest(tableName);
                request.setQuery(new TermQueryBuilder("entId",entId))
                        .setScript(new Script(ScriptType.INLINE,
                                "painless",
                                "ctx._source."+field,
                                Collections.<String, Object>emptyMap()));
                client.updateByQuery(request,RequestOptions.DEFAULT);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 写入elastic 文档
    public static void post(String tableName, String content) throws IOException {
        RestHighLevelClient client = creatClient();

        IndexRequest indexRequest = new IndexRequest(tableName);
        indexRequest.source(content, XContentType.JSON);
        IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(response.getId());
//        client.close();
    }

    public static List<String> postBatch(String tableName, List<String> jsonList) throws IOException {
        RestHighLevelClient client = creatClient();
        BulkRequest request = new BulkRequest();
        for (String json : jsonList) {
            IndexRequest indexRequest = new IndexRequest(tableName);
            indexRequest.opType(DocWriteRequest.OpType.CREATE);
            indexRequest.source(json, XContentType.JSON);
            request.add(indexRequest);
        }
        BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
        BulkItemResponse[] responses = response.getItems();
        List<String> resultList = Lists.newArrayList();
        for (BulkItemResponse bulkItemResponse : responses) {
            resultList.add((bulkItemResponse.getId()));
        }
//        client.close();
        return resultList;
    }
}