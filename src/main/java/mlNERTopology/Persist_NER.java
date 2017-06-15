package mlNERTopology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

/**
 * Created by rilfi on 3/19/2017.
 */
public class Persist_NER extends BaseRichBolt {
    OutputCollector _collector;
    int count;
    ArrayList<String>num=new ArrayList<>();

   // private BufferedWriter writer;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this._collector = outputCollector;
        /*try {
            writer=new BufferedWriter(new FileWriter("n.out"));
        } catch (IOException e) {
            e.printStackTrace();
        }*/

    }

    @Override
    public void execute(Tuple tuple) {
        count++;
        Map<String,String> returnMap= (Map<String, String>) tuple.getValueByField("returnMap");
        num.add(returnMap.get("MSGID"));

/*        Map<String,String> returnMap= (Map<String, String>) tuple.getValueByField("returnMap");
        String tags[]={"MOD","STA","GRO","BRA","CAT"};

        String line="";

        for (String tag:returnMap.keySet()) {
            String key = tag + ":"+returnMap.get(tag);


            line=line+key+"; ";


        }*/



        /*try {
            writer.write(line);
            writer.newLine();
            writer.flush();
        } catch (IOException e1) {
            e1.printStackTrace();
        }*/
        _collector.ack(tuple);




    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        System.out.println("*******************");
        System.out.println(count);
        System.out.println("*******************");
        for(String s:num){
            System.out.println(s);
        }
       /* try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }*/
    }
}

