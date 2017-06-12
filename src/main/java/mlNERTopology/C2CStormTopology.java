package mlNERTopology;


import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class C2CStormTopology {
  public static void main(String[] args) {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("Title", new Title_rich_Spout(), 1);

    builder.setBolt("State", new State_rich_Bolt(), 1).shuffleGrouping("Title");
    builder.setBolt("NER", new NER_rich_Bolt(), 4).shuffleGrouping("State");
    builder.setBolt("Model", new Model_NER_rich_Bolt(), 4).shuffleGrouping("NER");
    builder.setBolt("Group", new Group_rich_Bolt(), 1).shuffleGrouping("Model");
    builder.setBolt("persist", new Group_rich_Bolt(), 1).shuffleGrouping("Group");

    Config config = new Config();
    config.setDebug(true);
    config.setNumWorkers(2);
      try {
          StormSubmitter.submitTopology("C2CStormTopology", config, builder.createTopology());
      } catch (AlreadyAliveException e) {
          e.printStackTrace();
      } catch (InvalidTopologyException e) {
          e.printStackTrace();
      } catch (AuthorizationException e) {
          e.printStackTrace();
      }



    /*LocalCluster localCluster = new LocalCluster();
    localCluster.submitTopology("credit-card-rich_topology", config, builder.createTopology());*/

/*    Utils.sleep(10000);
    localCluster.shutdown();*/
  }
}