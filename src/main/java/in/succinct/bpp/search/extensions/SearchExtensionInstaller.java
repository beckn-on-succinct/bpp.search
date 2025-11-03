package in.succinct.bpp.search.extensions;

import com.venky.core.string.StringUtil;
import com.venky.core.util.ObjectUtil;
import com.venky.extension.Extension;
import com.venky.extension.Registry;
import com.venky.swf.db.Database;
import com.venky.swf.db.annotations.column.ui.mimes.MimeType;
import com.venky.swf.db.model.application.Application;
import com.venky.swf.integration.api.Call;
import com.venky.swf.integration.api.HttpMethod;
import com.venky.swf.integration.api.InputFormat;
import com.venky.swf.path._IPath;
import com.venky.swf.plugins.background.core.DbTask;
import com.venky.swf.plugins.background.core.TaskManager;
import com.venky.swf.routing.Config;
import in.succinct.beckn.Catalog;
import in.succinct.beckn.Context;
import in.succinct.beckn.Message;
import in.succinct.beckn.Providers;
import in.succinct.beckn.Request;
import in.succinct.beckn.Subscriber;
import in.succinct.bpp.core.adaptor.CommerceAdaptor;
import in.succinct.bpp.core.adaptor.api.NetworkApiAdaptor;
import in.succinct.onet.core.adaptor.NetworkAdaptor;
import in.succinct.onet.core.adaptor.NetworkAdaptor.Domain;
import org.json.simple.JSONAware;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;

public class SearchExtensionInstaller implements Extension {
    static {
        Registry.instance().registerExtension("in.succinct.bpp.search.extension.installer",new SearchExtensionInstaller());
        Registry.instance().registerExtension("in.succinct.bpp.search.extension.reinstall",new SearchExtensionInstaller());
    }
    @Override
    public void invoke(Object... context) {
        NetworkAdaptor networkAdaptor = (context.length > 0) ? (NetworkAdaptor) context[0] : null;
        CommerceAdaptor adaptor = (context.length > 1) ? (CommerceAdaptor) context[1] :null;
        String providerId = (context.length > 2) ? (String) context[2] :null;
        Application application = (context.length > 3) ? (Application)context[3] : null;
        if (adaptor != null) {
            indexItems(networkAdaptor, adaptor,providerId,application);
        }
    }

    private void indexItems(NetworkAdaptor networkAdaptor,CommerceAdaptor adaptor,String providerId,Application application) {
        String subscriberId = adaptor.getSubscriber().getSubscriberId();
        Request response = new Request();
        ((NetworkApiAdaptor)networkAdaptor.getApiAdaptor())._search(adaptor,providerId,response);
        Providers providers = response.getMessage().getCatalog().getProviders();


        Map<String,Object> attributes = Database.getInstance().getCurrentTransaction().getAttributes();
        Map<String,Object> context = Database.getInstance().getContext();

        TaskManager.instance().executeAsync((DbTask)()->{
            Database.getInstance().getCurrentTransaction().setAttributes(attributes);
            if (context != null) {
                context.remove(_IPath.class.getName());
                Database.getInstance().setContext(context);
            }
            Request request = prepareCatalogSyncRequest(providers,adaptor,networkAdaptor);
            Request networkRequest = networkAdaptor.getObjectCreator(adaptor.getSubscriber().getDomain()).create(Request.class);
            networkRequest.update(request); // Updates in network format
            call_bg(adaptor,getGateways(networkAdaptor),networkRequest,new HashMap<>());
            
        },false);

    }
    
    static List<Subscriber> getGateways(NetworkAdaptor networkAdaptor){
        return networkAdaptor.lookup(new in.succinct.beckn.Subscriber() {{
            //setSubscriberId(networkAdaptor.getSearchProviderId());
            setType(Subscriber.SUBSCRIBER_TYPE_BG);
        }}, true);
    }
    
    static <J extends JSONAware> List<J> call_bg(CommerceAdaptor adaptor , List<Subscriber> gateways, Request request, Map<String,String> additionalHeaders){
        List<J> responses = new ArrayList<>();
        for (Subscriber gwSubscriber : gateways) {
            if (!ObjectUtil.equals(gwSubscriber.getStatus(), Subscriber.SUBSCRIBER_STATUS_SUBSCRIBED)) {
                continue;
            }
            Call<InputStream> call = new Call<InputStream>().url(gwSubscriber.getSubscriberUrl(), request.getContext().getAction()).headers(additionalHeaders).
                    headers(new HashMap<>(){{
                        put("Content-Type", MimeType.APPLICATION_JSON.toString());
                        if (request.getContext().getAction().equals("on_search")) {
                            put("Authorization", request.generateAuthorizationHeader(adaptor.getSubscriber().getSubscriberId(), adaptor.getSubscriber().getPubKeyId()));
                        }
                    }}).inputFormat(InputFormat.INPUT_STREAM).method(HttpMethod.POST).input(new ByteArrayInputStream(request.toString().getBytes(StandardCharsets.UTF_8)));
            if (!call.hasErrors()) {
                Config.instance().getLogger(Catalog.class.getName()).log(Level.INFO, String.format("BroadCasted %s to bg %s :\n%s", request.getContext().getAction(), gwSubscriber.getSubscriberId(),
                        StringUtil.valueOf(call.getResponseStream())));
                responses.add(call.getResponseAsJson());
            } else {
                responses.add(null);
            }
        }
        return responses;
    }
    
    public Request prepareCatalogSyncRequest(Providers providers,CommerceAdaptor adaptor,NetworkAdaptor networkAdaptor){
        Request request = new Request();
        Context context = new Context();
        request.setContext(context);
        request.setMessage(new Message());
        request.getMessage().setCatalog(new Catalog());
        request.getMessage().getCatalog().setProviders(providers);
        context.setBppId(adaptor.getSubscriber().getSubscriberId());
        context.setBppUri(adaptor.getSubscriber().getSubscriberUrl());
        context.setTransactionId(UUID.randomUUID().toString());
        context.setMessageId(UUID.randomUUID().toString());
        context.setDomain(adaptor.getSubscriber().getDomain());
        context.setCountry(adaptor.getSubscriber().getCountry());
        context.setCoreVersion(networkAdaptor.getCoreVersion());
        context.setTimestamp(new Date());
        context.setNetworkId(networkAdaptor.getId());
        context.setCity(adaptor.getSubscriber().getCity());
        context.setTtl(60L);
        context.setAction("on_search");

        for (in.succinct.beckn.Provider provider : providers){
            provider.setTag("general_attributes","catalog.indexer.reset","Y");
        }
        return request;

    }
    
}
