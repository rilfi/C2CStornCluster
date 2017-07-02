package fileTest;

import mlNERTopology.DataPublisherUtil;
import org.wso2.carbon.databridge.agent.AgentHolder;
import org.wso2.carbon.databridge.agent.DataPublisher;
import org.wso2.carbon.databridge.agent.endpoint.thrift.ThriftDataEndpoint;
import org.wso2.carbon.databridge.agent.exception.DataEndpointAgentConfigurationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointAuthenticationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointConfigurationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointException;
import org.wso2.carbon.databridge.commons.Event;
import org.wso2.carbon.databridge.commons.exception.TransportException;

/**
 * Created by s1 on 7/2/2017.
 */
public class DasTest {


    public static void main(String[] args) {
        AgentHolder. setConfigPath ("models/conf.xml");
        DataPublisherUtil.setTrustStoreParams();
        //dataPublisher =  new  DataPublisher(url, username, password);
        DataPublisher dataPublisher = null;
/*        String protocol;
        String host;
        String port;
        String username;
        String password;
        String streamId;*/
        String protocol = "thrift";
        String host = "192.248.8.248";
        String port = "7611";
        String username = "admin";
        String password = "admin";
        String streamId = "justfortest:1.0.0";
        try {
            dataPublisher = new DataPublisher(protocol, "tcp://" + host + ":" + port, null, username, password);

        } catch (DataEndpointAgentConfigurationException e) {
            e.printStackTrace();
        } catch (DataEndpointException e) {
            e.printStackTrace();
        } catch (DataEndpointConfigurationException e) {
            e.printStackTrace();
        } catch (DataEndpointAuthenticationException e) {
            e.printStackTrace();
        } catch (TransportException e) {
            e.printStackTrace();
        }

        Object payloadDataArray[]={10,"a"};
        Event event = new Event(streamId, System.currentTimeMillis(), null, null, payloadDataArray);
        dataPublisher.publish(event);
        try {
            dataPublisher.shutdownWithAgent();
        } catch (DataEndpointException e) {
            e.printStackTrace();
        }
        System.out.println("dhjbsdfh");





    }
}
