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
public class Persist_rich_Bolt1 extends BaseRichBolt {
    OutputCollector _collector;
    private BufferedWriter writer;
    int count=0;

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
       // String row=tuple.getStringByField("row");
        _collector.ack(tuple);
        count++;


       /* try {
            writer.write(row);
            writer.newLine();
        } catch (IOException e1) {
            e1.printStackTrace();
        }*/
        //writer.write(tv.toString());




    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        System.out.println("ttttttttttttttttttttttttttttttttttttttt");
        System.out.println(count);
       /* try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }*/
    }
}

