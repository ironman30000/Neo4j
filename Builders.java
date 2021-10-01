import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;
import org.apache.http.HttpHost;
//import org.apache.http.HttpRequest;
import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.json.*;
import org.logicng.formulas.Formula;
import org.logicng.formulas.FormulaFactory;
import org.logicng.io.parsers.ParserException;
import org.logicng.io.parsers.PropositionalParser;

import javax.json.JsonObject;
import javax.json.stream.JsonParser;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main (String[]args) throws IOException, InterruptedException, JSONException, ParserException {



        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));

       CreateIndexRequest request = new CreateIndexRequest("sampleindex");
       request.settings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 2));
       CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
       System.out.println("response id: " + createIndexResponse.index());

       IndexRequest indexRequest = new IndexRequest("sampleindex");
       indexRequest.id("001");
       indexRequest.source("SampleKey","SampleValue");
       IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
       System.out.println("response id: "+indexResponse.getId());
       System.out.println("response name: "+indexResponse.getResult().name());

       HashMap<String, Integer> map = new HashMap<String, Integer>();
       map.put("keyOne", 10);
       map.put("keyTwo", 30);
       map.put("KeyThree", 20);

 //      IndexRequest indexRequest = new IndexRequest("sampleindex");
       indexRequest.id("002");
       indexRequest.source(map);
       IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
       System.out.println("response id: "+indexResponse.getId());
       System.out.println("response name: "+indexResponse.getResult().name());

       EmployeePojo emp = new EmployeePojo("Elon", "Musk");
       ObjectMapper obj =new ObjectMapper();
       String s=obj.writerWithDefaultPrettyPrinter().writeValueAsString(emp);
   //    System.out.println(s);


       IndexRequest indexRequest = new IndexRequest("sampleindex");
       indexRequest.id("003");
       indexRequest.source(new ObjectMapper().writeValueAsString(emp), XContentType.JSON);
       IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
       System.out.println("response id: "+indexResponse.getId());
       System.out.println("response name: "+indexResponse.getResult().name());

      
           //.  matchquerybuilder

//         SearchRequest searchRequest = new SearchRequest("kibana_sample_data_ecommerce");
//         SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//         searchSourceBuilder.query(QueryBuilders.matchAllQuery());
//         searchRequest.source(searchSourceBuilder);
//         SearchResponse response = client.search(searchRequest,RequestOptions.DEFAULT);
//         System.out.println(response.status().getStatus());
//         System.out.println(response.getHits());
//         SearchHits hits = response.getHits();
//         SearchHit[] searchHits = hits.getHits();
//         for(SearchHit i : searchHits ){
//             System.out.println(i);
//         }

        
            //.  termquerybuilder 
          
//         SearchRequest searchRequest = new SearchRequest("kibana_sample_data_ecommerce");
//         SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//         searchSourceBuilder.query(QueryBuilders.termQuery("day_of_week","Wednesday"));
//         searchSourceBuilder.sort(new FieldSortBuilder("products.base_price").order(SortOrder.ASC));
//         searchRequest.source(searchSourceBuilder);

//         SearchResponse response = client.search(searchRequest,RequestOptions.DEFAULT);
//         System.out.println(response.status().getStatus());
//         System.out.println(response.getHits());

//         SearchHits hits = response.getHits();
//         SearchHit[] searchHits = hits.getHits();

//         System.out.println(searchHits.length);
//         for(SearchHit i : searchHits ){
//             System.out.println(i);
//         }

     //////   //.   BoolQueryBuilder
        
 
        SearchRequest searchRequest = new SearchRequest("kibana_sample_data_ecommerce");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("day_of_week","Wednesday")));
        searchSourceBuilder.query(QueryBuilders.boolQuery().mustNot(QueryBuilders.matchQuery("customer_first_name","Yasmine")));
        searchSourceBuilder.sort(new FieldSortBuilder("products.base_price").order(SortOrder.ASC));
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest,RequestOptions.DEFAULT);
        System.out.println(response.status().getStatus());

        SearchHits hits = response.getHits();
        SearchHit[] searchHits = hits.getHits();

        System.out.println(searchHits.length);
        for(SearchHit i : searchHits ){
            System.out.println(i);
        }


    }

}
