package in.succinct.bpp.search.extensions;

import com.venky.extension.Extension;
import com.venky.extension.Registry;
import com.venky.swf.db.model.Model;
import com.venky.swf.db.model.application.Application;
import in.succinct.bpp.core.adaptor.CommerceAdaptor;
import in.succinct.catalog.indexer.db.model.Provider;
import in.succinct.onet.core.adaptor.NetworkAdaptor;

public class SearchIndexRebuilderExtension implements Extension {

    static {
        Registry.instance().registerExtension("in.succinct.bpp.search.extension.reinstall",new SearchIndexRebuilderExtension());
    }

    @Override
    public void invoke(Object... context) {
        NetworkAdaptor networkAdaptor = (context.length > 0) ? (NetworkAdaptor) context[0] : null;
        CommerceAdaptor adaptor = (context.length > 1) ? (CommerceAdaptor) context[1] :null;
        Application application = (context.length > 2) ? (Application)context[2] : null;
        if (adaptor != null){
            Provider.findBySubscriberId(adaptor.getSubscriber().getSubscriberId()).forEach(Model::destroy);
            adaptor.clearCache();
            Registry.instance().callExtensions("in.succinct.bpp.search.extension.installer",networkAdaptor,adaptor,application);
        }

    }
}
