package mlNERTopology;

import com.aliasi.chunk.Chunk;
import com.aliasi.chunk.Chunking;
import com.aliasi.crf.ChainCrfChunker;
import com.aliasi.util.AbstractExternalizable;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by a1 on 4/2/2017.
 */
public class Brand_NER_rich_Bolt extends BaseRichBolt {
    OutputCollector _collector;
    File modelFile ;
    ChainCrfChunker crfChunker;
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
        modelFile = new File("/root/models/brand_Product_crf.model");
        try {
            crfChunker= (ChainCrfChunker)AbstractExternalizable.readObject(modelFile);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        long beforeProcessTS = System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000);

        Map<String,String> returnMap= (Map<String, String>) tuple.getValueByField("returnMap");

        String msg=returnMap.get("MSG");
        Chunking chunking = crfChunker.chunk(msg);
        Set<String>brandSet=new HashSet<>();
        Set<String>catSet=new HashSet<>();
        for(Chunk el:chunking.chunkSet()){
            int start=el.start();
            int end=el.end();
            String chuntText= (String) chunking.charSequence().subSequence(start,end);
            String type=el.type();
            if(type.equals("brand")){
                brandSet.add(chuntText.toLowerCase());
            }
           /* else if(type.equals("CAT")){
                catSet.add(chuntText.toLowerCase());
            }*/
        }
        if(brandSet.size()>0){

            returnMap.put("BND",brandSet.toString());

        }
        else {
            returnMap.put("BND","noValue");

        }
        if (catSet.size()>0){
            returnMap.put("CAT",catSet.toString());
        }
        Long afterProcessTS = System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000);
        long averageTS = (afterProcessTS - initiatatedTime) / count;
        count++;
        long timeTaken = afterProcessTS - beforeProcessTS;



        returnMap.put("TT_NER",String.valueOf(timeTaken));
        returnMap.put("AV_NER",String.valueOf(averageTS));
        returnMap.put("TID_NER",String.valueOf(threadid));
        _collector.emit( tuple,new Values(returnMap));

        _collector.ack(tuple);




    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("returnMap"));

    }







}

