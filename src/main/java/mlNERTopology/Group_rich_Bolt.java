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
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by a1 on 4/2/2017.
 */
public class Group_rich_Bolt extends BaseRichBolt {
    OutputCollector _collector;
    File modelFile ;
    LogisticRegressionClassifier<CharSequence> classifier;


    private String row;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        modelFile =new File(getClass().getResource("group.model.LogReg").getFile());
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

        row=tuple.getStringByField("row");
        Classification classification
                = classifier.classify(row);
        String group=classification.bestCategory();
        Set<String>groupSet=new HashSet<>();
        Map<String,Set<String>> returnMap= (Map<String, Set<String>>) tuple.getValueByField("returnMap");
        groupSet.add(group);
        returnMap.put("GRO",groupSet);
        _collector.emit( tuple,new Values(row,returnMap));
        _collector.ack(tuple);




    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("row","returnMap"));

    }







}

