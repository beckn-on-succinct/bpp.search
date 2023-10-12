package in.succinct.bpp.search.extensions;

import com.venky.core.util.ObjectUtil;
import com.venky.extension.Extension;
import com.venky.extension.Registry;
import com.venky.swf.db.model.Model;
import com.venky.swf.db.model.application.Application;
import in.succinct.bpp.core.adaptor.CommerceAdaptor;
import in.succinct.bpp.core.adaptor.NetworkAdaptor;

public class SearchIndexRebuilderExtension implements Extension {

    static {
        Registry.instance().registerExtension("in.succinct.bpp.search.extension.reinstall",new SearchIndexRebuilderExtension());
    }

    @Override
    public void invoke(Object... context) {
        NetworkAdaptor networkAdaptor = (context.length > 0) ? (NetworkAdaptor) context[0] : null;
        CommerceAdaptor adaptor = (context.length > 1) ? (CommerceAdaptor) context[1] :null;
        Application application = (context.length > 2) ? (Application)context[2] : null;
        if (application == null){
            if (adaptor != null) {
                application = adaptor.getApplication();
            }
        }
        if (application !=null ){
            application.getRawRecord().getAsProxy(in.succinct.bpp.search.db.model.Application.class).getProviders().forEach(p->{
                if (ObjectUtil.equals(p.getObjectId(),adaptor.getSubscriber().getSubscriberId())){
                    //Provider id is subscriber id in AbstractECommerceAdaptor!!
                    p.destroy();
                }
            });
            adaptor.clearCache();
            Registry.instance().callExtensions("in.succinct.bpp.search.extension.installer",networkAdaptor,adaptor,application);
        }

    }
}
