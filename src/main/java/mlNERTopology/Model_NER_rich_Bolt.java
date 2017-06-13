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
    private long initiatatedTime;
    private long threadid;
    private long count;


    private String row;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        initiatatedTime = System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000);
        threadid=Thread.currentThread().getId();
        count = 1;

    }

    @Override
    public void execute(Tuple tuple) {
        long beforeProcessTS = System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000);


        Map<String,String> returnMap= (Map<String, String>) tuple.getValueByField("returnMap");

        String msg=returnMap.get("MSG");
        Set<String>modelSet=new HashSet<>();
        for(String token:msg.split(" ")){
            if(isAlphanumeric(token)){
                modelSet.add(token);
            }

        }
        if(modelSet.size()>0){
            returnMap.put("MOD",modelSet.toString());
        }
        Long afterProcessTS = System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000);
        long averageTS = (afterProcessTS - initiatatedTime) / count;
        count++;
        long timeTaken = afterProcessTS - beforeProcessTS;



        returnMap.put("TT_MOD",String.valueOf(timeTaken));
        returnMap.put("AV_MOD",String.valueOf(averageTS));
        returnMap.put("TID_MOD",String.valueOf(threadid));

        _collector.emit( tuple,new Values(returnMap));

        _collector.ack(tuple);




    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("returnMap"));

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

