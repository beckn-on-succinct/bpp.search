package in.succinct.bpp.search.agent;

import com.venky.swf.db.Database;
import com.venky.swf.plugins.background.core.Task;
import com.venky.swf.plugins.background.core.agent.AgentFinishUpTask;
import com.venky.swf.plugins.background.core.agent.AgentSeederTask;
import com.venky.swf.plugins.background.core.agent.AgentSeederTaskBuilder;
import com.venky.swf.plugins.beckn.messaging.Subscriber;
import com.venky.swf.plugins.beckn.tasks.BecknTask;
import com.venky.swf.sql.Select;
import in.succinct.beckn.Request;
import in.succinct.bpp.core.adaptor.CommerceAdaptor;
import in.succinct.bpp.core.adaptor.CommerceAdaptorFactory;
import in.succinct.bpp.core.adaptor.NetworkApiAdaptor;
import in.succinct.bpp.search.db.model.IncrementalSearchRequest;
import in.succinct.json.JSONAwareWrapper;
import in.succinct.onet.core.adaptor.NetworkAdaptorFactory;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IncrementalSearchProcessor implements Task , AgentSeederTaskBuilder{
    long id  =-1 ;
    public IncrementalSearchProcessor(){

    }
    public IncrementalSearchProcessor(long id){
        this.id = id;
    }
    @Override
    public void execute() {
        IncrementalSearchRequest incrementalSearchRequest = Database.getTable(IncrementalSearchRequest.class).get(this.id);
        if (incrementalSearchRequest != null){
            Subscriber subscriber = new Subscriber(incrementalSearchRequest.getSubscriberJson()) {
                @Override
                public <T extends BecknTask> Class<T> getTaskClass(String s) {
                    return null;
                }
            };
            subscriber.setAppId(incrementalSearchRequest.getAppId());

            JSONObject properties = JSONAwareWrapper.parse(incrementalSearchRequest.getCommerceAdaptorProperties());
            JSONObject headers = JSONAwareWrapper.parse(incrementalSearchRequest.getHeaders());
            Map<String,String> adaptorProperties = new HashMap<>();

            //noinspection unchecked
            properties.forEach((k,v)->{
                adaptorProperties.put(String.valueOf(k),String.valueOf(v));
            });


            Request internalRequest = new Request(incrementalSearchRequest.getRequestPayload());
            internalRequest.getExtendedAttributes().set("headers",headers);
            Request internalResponse = new Request();
            NetworkApiAdaptor apiAdaptor = (NetworkApiAdaptor) NetworkAdaptorFactory.getInstance().getAdaptor(incrementalSearchRequest.getNetworkId()).getApiAdaptor();
            apiAdaptor.createReplyContext(subscriber,internalRequest,internalResponse);

            CommerceAdaptor commerceAdaptor = CommerceAdaptorFactory.getInstance().createAdaptor(adaptorProperties,subscriber);
            commerceAdaptor.search(internalRequest,internalResponse);

            apiAdaptor.callback(commerceAdaptor,internalResponse);

        }
    }


    @Override
    public AgentSeederTask createSeederTask() {
        return new AgentSeederTask() {
            @Override
            public List<Task> getTasks() {
                Select select =new Select("ID").from(IncrementalSearchRequest.class);
                select.add(" where end_time  is null or end_time  > last_transmission_time");
                List<Task> tasks = new ArrayList<>();
                for (IncrementalSearchRequest incrementalSearchRequest : select.execute(IncrementalSearchRequest.class)){
                    tasks.add(new IncrementalSearchProcessor(incrementalSearchRequest.getId()));
                }
                tasks.add(new AgentFinishUpTask(AGENT_NAME));
                return tasks;
            }

            @Override
            public String getAgentName() {
                return AGENT_NAME;
            }
        };
    }

    public static final String AGENT_NAME = "INCREMENTAL_SEARCH";
}
