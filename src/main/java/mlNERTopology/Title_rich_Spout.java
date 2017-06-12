package mlNERTopology;



import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.*;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by rilfi on 3/19/2017.
 */
public class Title_rich_Spout extends BaseRichSpout {
  //  private static final Logger LOGGER = LogManager.getLogger(File_rich_Spout.class);

    private SpoutOutputCollector outputCollector;
    FileInputStream fis;
    InputStreamReader isr;
    BufferedReader br;
    private AtomicLong linesRead;


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        outputCollector = spoutOutputCollector;
       // String titleFile= (String)  map.get("fileName");
        linesRead = new AtomicLong(0);
        try {
            fis = new FileInputStream("mlNERTopology/c3TitleSet1.input");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        isr = new InputStreamReader(fis);
        br = new BufferedReader(isr);


    }

    @Override
    public void nextTuple() {
        String row = null;
        try {
            if ((row = br.readLine()) != null) {
                long id = linesRead.incrementAndGet();
                outputCollector.emit(new Values(row),id);
                Utils.sleep(1);


            }
            else {
                br.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }




    }

    @Override
    public void ack(Object msgId) {
        //.debug("Got ACK for msgId : " + msgId);
    }

    @Override
    public void fail(Object msgId) {
       // LOGGER.debug("Got FAIL for msgId : " + msgId);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("row"));

    }

}
