package mlNERTopology;

import com.aliasi.classify.Classification;
import com.aliasi.classify.LogisticRegressionClassifier;
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
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by a1 on 4/2/2017.
 */
public class State_rich_Bolt extends BaseRichBolt {
    OutputCollector _collector;
    File modelFile ;
    LogisticRegressionClassifier<CharSequence> classifier;
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


        modelFile =new File("/root/models/status.model.LogReg");

        //modelFile=input;

        try {
            classifier= (LogisticRegressionClassifier<CharSequence>) AbstractExternalizable.readObject(modelFile);
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
        Classification classification
                = classifier.classify(msg);
        String state=classification.bestCategory();
        Long afterProcessTS = System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000);
        long averageTS = (afterProcessTS - initiatatedTime) / count;
        count++;
        long timeTaken = afterProcessTS - beforeProcessTS;


        returnMap.put("STA",state);
        returnMap.put("TT_STA",String.valueOf(timeTaken));
        returnMap.put("AV_STA",String.valueOf(averageTS));
        returnMap.put("TID_STA",String.valueOf(threadid));
        _collector.emit( tuple,new Values(returnMap));
        _collector.ack(tuple);




    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("returnMap"));

    }







}

