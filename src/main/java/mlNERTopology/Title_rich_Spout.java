package mlNERTopology;



import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
    private long started;
    int count1;
    int count2;



    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        outputCollector = spoutOutputCollector;
        started = System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000);
        File f=new File("/root/1000.in");
        count1=0;
        count2=0;

        // String titleFile= (String)  map.get("fileName");
        linesRead = new AtomicLong(0);
        InputStream input = getClass().getResourceAsStream("1000.in");
        //fis = new FileInputStream("c3TitleSet1.input");
       // isr = new InputStreamReader(fis);
        try {
            br = new BufferedReader(new FileReader(f));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }


    }

    @Override
    public void nextTuple() {
        count1++;
        String row = null;
        try {
            while ((row = br.readLine()) != null) {
                long id = linesRead.incrementAndGet();
                Map<String,String>returnMap=new HashMap<>();
                returnMap.put("MSG",row);
                returnMap.put("STARTED",String.valueOf(started));
                returnMap.put("TPLSTART",String.valueOf(System.nanoTime() - (24 * 60 * 60 * 1000 * 1000 * 1000)));
                returnMap.put("MSGID",String.valueOf(id));
               Utils.sleep(100);
                outputCollector.emit(new Values(returnMap),id);
               // Utils.sleep(100);
                count2++;


            }






        } catch (IOException e) {
            e.printStackTrace();
        }
       /* try {
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }*/


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
        outputFieldsDeclarer.declare(new Fields("returnMap"));

    }
    @Override
    public void close(){
        System.out.println("*****************");
        System.out.println(count1);
        System.out.println("************************");
        System.out.println(count2);
    }

}
