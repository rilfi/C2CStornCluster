package mlNERTopology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.glassfish.jersey.SslConfigurator;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.json.JSONArray;

import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

/**
 * Created by rilfi on 3/19/2017.
 */
public class Related_Das_rich_Bolt extends BaseRichBolt {
    SslConfigurator sslConfig ;
    SSLContext sslContext ;
    //HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic("Basic", "YWRtaW46YWRtaW4=");
    HttpAuthenticationFeature feature ;
    Client client ;
    WebTarget webTarget;
    WebTarget webTargetSc;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        sslConfig = SslConfigurator.newInstance()
                .trustStoreFile("/root/models/client-truststore.jks")
                .trustStorePassword("wso2carbon")
                .keyStoreFile("/root/models/wso2carbon.jks")
                .keyPassword("wso2carbon");
        sslContext = sslConfig.createSSLContext();
        //HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic("Basic", "YWRtaW46YWRtaW4=");
        feature = HttpAuthenticationFeature.basic("admin", "admin");
        client = ClientBuilder.newBuilder().sslContext(sslContext).build();
        webTarget = client.target("https://localhost:9443").path("analytics/search").register(feature);
        webTargetSc=client.target("https://localhost:9443").path("analytics/search_count").register(feature);

    }
    public  String getRelatedRecords(String brand, String product, String model, String status, String group){

        String payload = "{\"tableName\":\"INPUTSTREAMTOPERSIST\",\"query\":\"brand:"+brand+"\",\"product:" + product + "\",\"model:" + model + "\",\"status:" + status + "\",\"group:" + group + "\"}";
        Response response = webTarget.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(payload));
        return response.readEntity(String.class);

    }

    @Override
    public void execute(Tuple tuple) {
        Map<String,String> returnMap= (Map<String, String>) tuple.getValueByField("returnMap");
        String sta=returnMap.get("STA");
        if(sta.equals("s")){
            sta="b";
        }
        else if (sta.equals("b")){
            sta="s";
    }
    JSONArray ja = new JSONArray(getRelatedRecords(returnMap.get("BND"),returnMap.get("PRO"),returnMap.get("MOD"),sta,returnMap.get("GRO")));


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
