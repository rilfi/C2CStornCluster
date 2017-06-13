package mlNERTopology;

import com.aliasi.crf.ChainCrfChunker;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by a1 on 4/2/2017.
 */
public class Model_NER_rich_Bolt extends BaseRichBolt {
    OutputCollector _collector;


    private String row;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {

        row=tuple.getStringByField("row");
        Map<String,Set<String>> returnMap= (Map<String, Set<String>>) tuple.getValueByField("returnMap");
        Set<String>modelSet=new HashSet<>();
        for(String token:row.split(" ")){
            if(isAlphanumeric(token)){
                modelSet.add(token);
            }

        }
        if(modelSet.size()>0){
            returnMap.put("MOD",modelSet);
        }

        _collector.emit( tuple,new Values(row,returnMap));

        _collector.ack(tuple);




    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("row","returnMap"));

    }

    public boolean isAlphanumeric(String str) {
        boolean isAlpha=false;
        boolean isNumaric=false;
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (Character.isDigit(c) ) {
                isNumaric = true;
                break;
            }
        }
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (Character.isLetter(c)) {
                isAlpha = true;
                break;
            }
        }
        if(isAlpha&&isNumaric){
            return true;
        }

        return false;
    }







}

