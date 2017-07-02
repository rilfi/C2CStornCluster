package mlNERTopology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Array;
import java.util.*;

/**
 * Created by rilfi on 3/19/2017.
 */
public class SplitMsg_Bolt extends BaseRichBolt {
    OutputCollector _collector;
    private BufferedWriter writer;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;


    }

    @Override
    public void execute(Tuple tuple) {
        Map<String,String> returnMap= (Map<String, String>) tuple.getValueByField("returnMap");
        String brandAr[]= returnMap.get("BRA").replace("[","").replace("]","").split(",");
        String productAr[]=returnMap.get("CAT").replace("[","").replace("]","").split(",");
        String modelAr[]=returnMap.get("MOD").replace("[","").replace("]","").split(",");
        String status=returnMap.get("STA");
        String group=returnMap.get("GRO");
        List<String> brandList= Arrays.asList(brandAr);
        List<String>productList=Arrays.asList(productAr);
        List<String>modelList=Arrays.asList(modelAr);
        String tagAr[]={"BRA","CAT","MOD"};
        List<String>tagList=Arrays.asList(tagAr);
        for(String br:brandAr){
            for (String pr:productAr){
                for(String mr:modelAr){
                    Map<String,String> eventMap=new HashMap<>();
                    eventMap.put("BRA",br);
                    eventMap.put("PRO",pr);
                    eventMap.put("MOD",mr);
                    for(String key:returnMap.keySet()){
                        if(tagList.contains(key)){
                            continue;
                        }
                        eventMap.put(key,returnMap.get(key));

                    }
                    _collector.emit( tuple,new Values(eventMap));


                }
            }
        }


        _collector.ack(tuple);




    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

