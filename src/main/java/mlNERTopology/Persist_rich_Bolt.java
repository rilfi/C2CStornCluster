package mlNERTopology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Created by rilfi on 3/19/2017.
 */
public class Persist_rich_Bolt extends BaseRichBolt {
    OutputCollector _collector;
    private BufferedWriter writer;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        try {
            writer=new BufferedWriter(new FileWriter("tags.out"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void execute(Tuple tuple) {
        Map<String,Set<String>> returnMap= (Map<String, Set<String>>) tuple.getValueByField("returnMap");
        String tags[]={"MOD","STA","GRO","BRA","CAT"};

        String line="";

        for (String tag:returnMap.keySet()) {
            String key = tag + ":";
            for (String val : returnMap.get(tag)) {
                key = key + val + ",";
            }
            line=line+key+"; ";


        }
        System.out.println(line);
        _collector.ack(tuple);


        try {
            writer.write(line);
            writer.newLine();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        //writer.write(tv.toString());




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

