package mlNERTopology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.wso2.carbon.databridge.agent.AgentHolder;
import org.wso2.carbon.databridge.agent.DataPublisher;
import org.wso2.carbon.databridge.agent.exception.DataEndpointAgentConfigurationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointAuthenticationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointConfigurationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointException;
import org.wso2.carbon.databridge.commons.Event;
import org.wso2.carbon.databridge.commons.exception.TransportException;

import java.io.IOException;
import java.util.Map;

/**
 * Created by rilfi on 3/19/2017.
 */
public class RT_Das_rich_Bolt extends BaseRichBolt {

    DataPublisher dataPublisher;
    String protocol;
    String host;
    String port;
    String username;
    String password;
    String streamId;
    String streamId1;
    String sampleNumber;
    OutputCollector _collector;
    int delay;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
                _collector = outputCollector;

        AgentHolder. setConfigPath ("/root/models/conf.xml");
        DataPublisherUtil.setTrustStoreParams();
        //dataPublisher =  new  DataPublisher(url, username, password);
        protocol = "thrift";
        host = "192.248.8.248";
        port = "7611";
        username = "admin";
        password = "admin";
        streamId = "RT_C2C_FULL1:1.0.0";
       //streamId = "justfortest:1.0.0";

        streamId1 = "RelatedStream:1.0.0";
        sampleNumber = "0007";
        try {
            dataPublisher = new DataPublisher(protocol, "tcp://" + host + ":" + port, null, username, password);
        } catch (DataEndpointAgentConfigurationException e) {
            e.printStackTrace();
        } catch (DataEndpointException e) {
            e.printStackTrace();
        } catch (DataEndpointConfigurationException e) {
            e.printStackTrace();
        } catch (DataEndpointAuthenticationException e) {
            e.printStackTrace();
        } catch (TransportException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void execute(Tuple tuple) {
        Map<String,String> returnMap= (Map<String, String>) tuple.getValueByField("returnMap");




       // Object metaDataArray[]={returnMap.get("MSG"),Long.getLong(returnMap.get("TID_NER")),Long.getLong(returnMap.get("TID_STA")),Long.getLong(returnMap.get("TID_MOD")),Long.getLong(returnMap.get("TID_GRO")),returnMap.get("USER"),returnMap.get("URL"),returnMap.get("LOC")};
       // Object correlationdataArray[]={Long.getLong(returnMap.get("STARTED")),Long.getLong(returnMap.get("TPLSTART")),Long.getLong(returnMap.get("TT_NER")),Long.getLong(returnMap.get("AV_NER")),Long.getLong(returnMap.get("TT_STA")),Long.getLong(returnMap.get("AV_STA")),Long.getLong(returnMap.get("TT_MOD")),Long.getLong(returnMap.get("AV_MOD")),Long.getLong(returnMap.get("TT_GRO")),Long.getLong(returnMap.get("AV_GRO")),Long.getLong(returnMap.get("CNT_NER")),Long.getLong(returnMap.get("CNT_STA")),Long.getLong(returnMap.get("CNT_MOD")),Long.getLong(returnMap.get("CNT_GRO"))};

        //Object correlationdataArray[]={returnMap.get("STARTED"),returnMap.get("TPLSTART"),returnMap.get("TT_NER"),returnMap.get("AV_NER"),returnMap.get("TT_STA"),returnMap.get("AV_STA"),returnMap.get("TT_MOD"),returnMap.get("AV_MOD"),returnMap.get("TT_GRO"),returnMap.get("AV_GRO"),returnMap.get("CNT_NER"),returnMap.get("CNT_STA"),returnMap.get("CNT_MOD"),returnMap.get("CNT_GRO")};
        Object correlationdataArray[]={1,2,3,4,5,6,7,8,9,10,11,12,13,14};
        Object metaDataArray[]={"t",1,2,3,4,"un","url","loc"};
        Object payloadDataArray[]={1,"b","p","m","s","g"};
      // Object payloadDataArray[]={Integer.parseInt(returnMap.get("MSGID")),returnMap.get("BND"),returnMap.get("PRO"),returnMap.get("MOD"),returnMap.get("STA"),returnMap.get("GRO")};
        //Object payloadDataArray[]={Integer.parseInt(returnMap.get("MSGID")),returnMap.get("MOD")};






        //Event event = new Event(streamId, System.currentTimeMillis(), metaDataArray, correlationdataArray, payloadDataArray);
        Event event = new Event(streamId, System.currentTimeMillis(), null, correlationdataArray, payloadDataArray);

        dataPublisher.publish(event);
        _collector.ack(tuple);


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
    @Override
    public void cleanup() {
        try {
            dataPublisher.shutdownWithAgent();
        } catch (DataEndpointException e) {
            e.printStackTrace();
        }
    }
}
