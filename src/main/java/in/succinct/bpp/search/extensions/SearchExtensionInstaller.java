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
import com.venky.swf.plugins.beckn.messaging.Subscriber;
import in.succinct.beckn.Request;
import in.succinct.bpp.core.adaptor.CommerceAdaptor;

import java.sql.Timestamp;

public class SearchExtensionInstaller implements Extension {
    static {
        Registry.instance().registerExtension("in.succinct.bpp.search.extension.installer",new SearchExtensionInstaller());
    }
    @Override
    public void invoke(Object... context) {
        CommerceAdaptor adaptor = (CommerceAdaptor) context[0];
        Application application = (Application)context[1];
        installPlugin(adaptor,application);
    }
    public void installPlugin(CommerceAdaptor adaptor,Application app){
        Subscriber subscriber = adaptor.getSubscriber();

        // Create App for self
        Application application  = app;
        if (application == null) {
            application = Database.getTable(Application.class).newRecord();
            application.setAppId(subscriber.getAppId());
            application.setHeaders("(created) (expires) digest");
            application.setSignatureLifeMillis(5000);
            application.setSigningAlgorithm(Request.SIGNATURE_ALGO);
            application.setHashingAlgorithm("BLAKE2B-512");
            application.setSigningAlgorithmCommonName(application.getSigningAlgorithm().toLowerCase());
            application.setHashingAlgorithmCommonName(application.getHashingAlgorithm().toLowerCase());
            application = Database.getTable(Application.class).getRefreshed(application);
            application.save();
        }


        //CryptoKey cryptoKey = CryptoKey.find(subscriber.getCryptoKeyId(), CryptoKey.PURPOSE_SIGNING);

        ApplicationPublicKey publicKey = Database.getTable(ApplicationPublicKey.class).newRecord();
        publicKey.setApplicationId(application.getId());
        publicKey.setPurpose(CryptoKey.PURPOSE_SIGNING);
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

        Event event = Database.getTable(Event.class).newRecord();
        event.setName(CATALOG_SYNC_EVENT);
        event = Database.getTable(Event.class).getRefreshed(event);
        event.save();

        EventHandler eventHandler = Database.getTable(EventHandler.class).newRecord();
        eventHandler.setApplicationId(application.getId());
        eventHandler.setEndPointId(endPoint.getId());
        eventHandler.setEventId(event.getId());
        eventHandler.setContentType(MimeType.APPLICATION_JSON.toString());
        eventHandler.setRelativeUrl("hook/providers/ingest"); //Dummy action needed to next from subscriber url. Url is needed to detect registry usually
        eventHandler = Database.getTable(EventHandler.class).getRefreshed(eventHandler);
        eventHandler.save();

        Registry.instance().callExtensions("in.succinct.bpp.search.extension.index.full",adaptor,application);
    }
    public static String CATALOG_SYNC_EVENT = "catalog_index";


}
