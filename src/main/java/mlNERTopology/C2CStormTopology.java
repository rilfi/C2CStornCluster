package mlNERTopology;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class C2CStormTopology {
  public static void main(String[] args) {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("Title", new Title_rich_Spout(), 1);
    builder.setBolt("NER", new NER_rich_Bolt(), 1).shuffleGrouping("Title");
    //builder.setBolt("np", new Persist_NER(),1 ).shuffleGrouping("NER");
    builder.setBolt("State", new State_rich_Bolt(),1 ).shuffleGrouping("NER");
    builder.setBolt("Model", new Model_NER_rich_Bolt(), 1).shuffleGrouping("State");
    builder.setBolt("Group", new Group_rich_Bolt(), 1).shuffleGrouping("Model");
    builder.setBolt("persist", new Persist_rich_Bolt(), 1).shuffleGrouping("Group");



/*    builder.setBolt("Group", new Group_rich_Bolt(), 1).shuffleGrouping("Title");
    builder.setBolt("gp", new Persist_Group() ,1 ).shuffleGrouping("Group");*/
  /*  builder.setBolt("tp", new Persist_title(),1 ).shuffleGrouping("Title");


    builder.setBolt("State", new State_rich_Bolt(),1 ).shuffleGrouping("Title");
    builder.setBolt("sp", new Persist_State(),1 ).shuffleGrouping("State");

    builder.setBolt("Model", new Model_NER_rich_Bolt(), 1).shuffleGrouping("Title");
    builder.setBolt("mp", new Persist_Model(),1 ).shuffleGrouping("Model");

    builder.setBolt("Group", new Group_rich_Bolt(), 1).shuffleGrouping("Title");
    builder.setBolt("gp", new Persist_Model(),1 ).shuffleGrouping("Group");

   /* builder.setBolt("NER", new NER_rich_Bolt(), 1).shuffleGrouping("Title");
    builder.setBolt("np", new Persist_NER(),1 ).shuffleGrouping("NER");*/


    //builder.setBolt("persist", new Persist_rich_Bolt(), 1).shuffleGrouping("NER");

    Config config = new Config();
    config.setDebug(true);
    //config.setNumWorkers(3);
/*      try {
          StormSubmitter.submitTopology("C2CStormTopology", config, builder.createTopology());
      } catch (AlreadyAliveException e) {
          e.printStackTrace();
      } catch (InvalidTopologyException e) {
          e.printStackTrace();
      } catch (AuthorizationException e) {
          e.printStackTrace();
      }*/



    LocalCluster localCluster = new LocalCluster();
    localCluster.submitTopology("C2CStormTopology1", config, builder.createTopology());

    Utils.sleep(100000);
    localCluster.shutdown();
  }
}