package in.succinct.bpp.search.extensions;

import com.venky.extension.Extension;
import com.venky.extension.Registry;
import com.venky.swf.db.model.Model;
import com.venky.swf.db.model.application.Application;
import com.venky.swf.sql.Expression;
import com.venky.swf.sql.Operator;
import com.venky.swf.sql.Select;
import in.succinct.bpp.core.adaptor.CommerceAdaptor;
import in.succinct.catalog.indexer.db.model.IndexedSubscriberModel;
import in.succinct.catalog.indexer.db.model.Provider;

import java.util.List;

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
        Provider.findBySubscriberId(adaptor.getSubscriber().getSubscriberId()).forEach(Model::destroy);
    }

}
