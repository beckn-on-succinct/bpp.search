package in.succinct.bpp.search.extensions;

import com.venky.core.util.ObjectUtil;
import com.venky.extension.Extension;
import com.venky.extension.Registry;
import com.venky.swf.db.Database;
import com.venky.swf.db.annotations.column.ui.mimes.MimeType;
import com.venky.swf.db.model.CryptoKey;
import com.venky.swf.db.model.application.Application;
import com.venky.swf.db.model.application.ApplicationPublicKey;
import com.venky.swf.db.model.application.Event;
import com.venky.swf.db.model.application.api.EndPoint;
import com.venky.swf.db.model.application.api.EventHandler;
import com.venky.swf.db.model.reflection.ModelReflector;
import com.venky.swf.path._IPath;
import com.venky.swf.plugins.background.core.DbTask;
import com.venky.swf.plugins.background.core.TaskManager;
import com.venky.swf.plugins.beckn.messaging.Subscriber;

import com.venky.swf.plugins.collab.db.model.participants.admin.Company;
import com.venky.swf.sql.Expression;
import com.venky.swf.sql.Operator;
import com.venky.swf.sql.Select;
import in.succinct.beckn.Providers;
import in.succinct.beckn.Request;
import in.succinct.bpp.core.adaptor.CommerceAdaptor;
import in.succinct.bpp.core.adaptor.NetworkAdaptor;
import in.succinct.bpp.search.db.model.Item;
import in.succinct.bpp.search.db.model.Provider;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SearchExtensionInstaller implements Extension {
    static {
        Registry.instance().registerExtension("in.succinct.bpp.search.extension.installer",new SearchExtensionInstaller());
    }
    @Override
    public void invoke(Object... context) {
        NetworkAdaptor networkAdaptor = (context.length > 0) ? (NetworkAdaptor) context[0] : null;
        CommerceAdaptor adaptor = (context.length > 1) ? (CommerceAdaptor) context[1] :null;
        Application application = (context.length > 2) ? (Application)context[2] : null;
        installPlugin(networkAdaptor,adaptor,application);
    }
    public void installPlugin(NetworkAdaptor networkAdaptor,CommerceAdaptor adaptor,Application app){
        Subscriber subscriber = adaptor.getSubscriber();

        // Create App for self
        Application application  = app;
        if (application == null) {
            Company company = adaptor.createCompany(adaptor.getProviderConfig().getOrganization(),adaptor.getSubscriber().getSubscriberId());
            application = adaptor.createApplication(company,adaptor.getSubscriber().getSubscriberId(),null);
        }


        //CryptoKey cryptoKey = CryptoKey.find(subscriber.getCryptoKeyId(), CryptoKey.PURPOSE_SIGNING);

        ApplicationPublicKey publicKey = Database.getTable(ApplicationPublicKey.class).newRecord();
        publicKey.setApplicationId(application.getId());
        publicKey.setPurpose(CryptoKey.PURPOSE_SIGNING);
        publicKey.setAlgorithm(Request.SIGNATURE_ALGO);
        publicKey.setKeyId(subscriber.getUniqueKeyId());
        publicKey.setValidFrom(new Timestamp(subscriber.getValidFrom().getTime()));
        publicKey.setValidUntil(new Timestamp(subscriber.getValidTo().getTime()));
        publicKey.setPublicKey(Request.getPemSigningKey(subscriber.getSigningPublicKey()));
        publicKey = Database.getTable(ApplicationPublicKey.class).getRefreshed(publicKey);
        publicKey.save();

        EndPoint endPoint = Database.getTable(EndPoint.class).newRecord();
        endPoint.setApplicationId(application.getId());
        endPoint.setBaseUrl(subscriber.getSubscriberUrl());
        endPoint = Database.getTable(EndPoint.class).getRefreshed(endPoint);
        endPoint.save();


        for (String eventName : new String[]{CATALOG_SYNC_EVENT,CATALOG_SYNC_ACTIVATE, CATALOG_SYNC_DEACTIVATE}){
            Event event = Database.getTable(Event.class).newRecord();
            event.setName(eventName);
            event = Database.getTable(Event.class).getRefreshed(event);
            event.save();

            EventHandler eventHandler = Database.getTable(EventHandler.class).newRecord();
            eventHandler.setApplicationId(application.getId());
            eventHandler.setEndPointId(endPoint.getId());
            eventHandler.setEventId(event.getId());
            eventHandler.setContentType(MimeType.APPLICATION_JSON.toString());
            eventHandler.setRelativeUrl(String.format("hook/providers/%s",eventName.substring("catalog_".length()))); //Dummy action needed to next from subscriber url. Url is needed to detect registry usually
            eventHandler = Database.getTable(EventHandler.class).getRefreshed(eventHandler);
            eventHandler.save();

        }
        indexItems(networkAdaptor,adaptor,application);
    }
    public static String CATALOG_SYNC_EVENT = "catalog_ingest";
    public static String CATALOG_SYNC_ACTIVATE = "catalog_activate";
    public static String CATALOG_SYNC_DEACTIVATE = "catalog_deactivate";

    private void indexItems(NetworkAdaptor networkAdaptor,CommerceAdaptor adaptor, com.venky.swf.db.model.application.Application application) {
        List<Provider> providerList = application.getRawRecord().getAsProxy(in.succinct.bpp.search.db.model.Application.class).
                getProviders().stream().filter(p-> ObjectUtil.equals(p.getObjectId(),adaptor.getSubscriber().getSubscriberId())).collect(Collectors.toList());
        if (!providerList.isEmpty()){
            new Select().from(Item.class).where(new Expression(ModelReflector.instance(Item.class).getPool(),"ACTIVE", Operator.EQ)).execute(Item.class).forEach(i->{
                i.setActive(true);i.save();
            });
            return;
        }

        Request response = new Request();
        networkAdaptor.getApiAdaptor()._search(adaptor,response);
        Providers providers = response.getMessage().getCatalog().getProviders();


        Map<String,Object> attributes = Database.getInstance().getCurrentTransaction().getAttributes();
        Map<String,Object> context = Database.getInstance().getContext();

        TaskManager.instance().executeAsync((DbTask)()->{
            Database.getInstance().getCurrentTransaction().setAttributes(attributes);
            if (context != null) {
                context.remove(_IPath.class.getName());
                Database.getInstance().setContext(context);
            }
            Event event = Event.find(CATALOG_SYNC_EVENT);
            if (event != null ){
                event.raise(providers);
            }
        },false);

    }

}
