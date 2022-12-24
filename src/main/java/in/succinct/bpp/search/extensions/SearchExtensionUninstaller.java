package in.succinct.bpp.search.extensions;

import com.venky.extension.Extension;
import com.venky.extension.Registry;
import com.venky.swf.db.Database;
import com.venky.swf.db.annotations.column.ui.mimes.MimeType;
import com.venky.swf.db.model.CryptoKey;
import com.venky.swf.db.model.application.Application;
import com.venky.swf.db.model.application.ApplicationPublicKey;
import com.venky.swf.db.model.application.ApplicationUtil;
import com.venky.swf.db.model.application.Event;
import com.venky.swf.db.model.application.api.EndPoint;
import com.venky.swf.db.model.application.api.EventHandler;
import com.venky.swf.plugins.beckn.messaging.Subscriber;
import in.succinct.beckn.Request;
import in.succinct.bpp.core.adaptor.CommerceAdaptor;

import java.sql.Timestamp;

public class SearchExtensionUninstaller implements Extension {
    static {
        Registry.instance().registerExtension("in.succinct.bpp.search.extension.uninstaller",new SearchExtensionUninstaller());
    }
    @Override
    public void invoke(Object... context) {
        CommerceAdaptor adaptor = (CommerceAdaptor) context[0];
        Application application = (Application)context[1];
        uninstallPlugin(adaptor,application);
    }
    public void uninstallPlugin(CommerceAdaptor adaptor,Application app){
        in.succinct.bpp.search.db.model.Application application = app.getRawRecord().getAsProxy(in.succinct.bpp.search.db.model.Application.class);
        application.getCategories().forEach(c->c.destroy());
        application.getFulfillments().forEach(f->f.destroy());
        application.getItems().forEach(i->i.destroy());
        application.getPayments().forEach(p->p.destroy());
        application.getProviders().forEach(p->p.destroy());
        application.getProviderLocations().forEach(l->l.destroy());
    }


}
