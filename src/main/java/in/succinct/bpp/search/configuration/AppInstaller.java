package in.succinct.bpp.search.configuration;

import com.venky.swf.configuration.Installer;
import com.venky.swf.db.Database;
import com.venky.swf.db.annotations.column.ui.mimes.MimeType;
import com.venky.swf.db.model.application.Application;
import com.venky.swf.db.model.application.ApplicationPublicKey;
import com.venky.swf.db.model.application.Event;
import com.venky.swf.db.model.application.api.EndPoint;
import com.venky.swf.db.model.application.api.EventHandler;
import com.venky.swf.plugins.collab.db.model.CryptoKey;
import com.venky.swf.routing.Config;
import in.succinct.beckn.Request;
import in.succinct.bpp.shell.util.BecknUtil;

import java.sql.Timestamp;

public class AppInstaller implements Installer {

    public void install() {
        installApplications();
    }

    private void installApplications() {
        Application application = Database.getTable(Application.class).newRecord();
        application.setAppId(BecknUtil.getSubscriberId());
        application.setHeaders("(created) (expires) digest");
        application.setSignatureLifeMillis(5000);
        application.setSigningAlgorithm(Request.SIGNATURE_ALGO);
        application.setHashingAlgorithm("BLAKE2B-512");
        application.setSigningAlgorithmCommonName(application.getSigningAlgorithm().toLowerCase());
        application.setHashingAlgorithmCommonName(application.getSigningAlgorithm().toLowerCase());
        application = Database.getTable(Application.class).getRefreshed(application);
        application.save();

        CryptoKey cryptoKey = CryptoKey.find(BecknUtil.getCryptoKeyId(), CryptoKey.PURPOSE_SIGNING);

        ApplicationPublicKey publicKey = Database.getTable(ApplicationPublicKey.class).newRecord();
        publicKey.setApplicationId(application.getId());
        publicKey.setPurpose(cryptoKey.getPurpose());
        publicKey.setKeyId(String.format("k%d", BecknUtil.getCurrentKeyNumber()));
        publicKey.setValidFrom(cryptoKey.getUpdatedAt());
        publicKey.setValidUntil(new Timestamp(publicKey.getValidFrom().getTime() + BecknUtil.getKeyValidityMillis()));
        publicKey.setPublicKey(cryptoKey.getPublicKey());
        publicKey = Database.getTable(ApplicationPublicKey.class).getRefreshed(publicKey);
        publicKey.save();

        EndPoint endPoint = Database.getTable(EndPoint.class).newRecord();
        endPoint.setApplicationId(application.getId());
        endPoint.setBaseUrl(Config.instance().getServerBaseUrl());
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
        eventHandler.setRelativeUrl("providers/ingest");
        eventHandler = Database.getTable(EventHandler.class).getRefreshed(eventHandler);
        eventHandler.save();
    }

    public static String CATALOG_SYNC_EVENT = "catalog_index";

}

