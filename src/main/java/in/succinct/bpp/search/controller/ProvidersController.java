package in.succinct.bpp.search.controller;

import com.venky.swf.controller.ModelController;
import com.venky.swf.db.Database;
import com.venky.swf.db.model.application.Application;
import com.venky.swf.integration.api.HttpMethod;
import com.venky.swf.path.Path;
import com.venky.swf.views.View;
import in.succinct.beckn.Item;
import in.succinct.beckn.Items;
import in.succinct.beckn.Providers;
import in.succinct.bpp.search.db.model.Provider;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.InputStreamReader;

public class ProvidersController extends ModelController<Provider> {
    public ProvidersController(Path path) {
        super(path);
    }

    public View activate(){
        return update(true);
    }
    public View deactivate(){
        return update(false);
    }
    public  View update(boolean active){
        try {
            in.succinct.beckn.Provider provider =
                    new in.succinct.beckn.Provider();
            provider.setInner((JSONObject) JSONValue.parse(new InputStreamReader(getPath().getInputStream())));
            update(provider,active);
            return getReturnIntegrationAdaptor().createStatusResponse(getPath(),null);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



    public Provider update(in.succinct.beckn.Provider bProvider, boolean active){
        String appId = getPath().getApplication().getAppId();

        Provider provider = Database.getTable(Provider.class).newRecord();
        provider.setActive(active);
        provider.setProviderJson(bProvider.toString());
        provider.setProviderName(bProvider.getDescriptor().getName());
        Item item = bProvider.getItems().get(0);
        provider.setItemName(item.getDescriptor().getName());
        provider.setItemCategoryName(bProvider.getCategories().get(item.getCategoryId()).getDescriptor().getName());
        provider.setProviderLocationName(bProvider.getLocations().get(item.getLocationId()).getDescriptor().getName());
        provider.setBppId(appId);
        provider = Database.getTable(Provider.class).getRefreshed(provider);
        provider.save();
        return provider;
    }

    public View ingest() throws Exception{
        ensureIntegrationMethod(HttpMethod.POST);

        Providers providers = new Providers((JSONArray) JSONValue.parse(new InputStreamReader(getPath().getInputStream())));
        for (int i = 0 ; i < providers.size() ; i ++){
            in.succinct.beckn.Provider provider = providers.get(i);

            Items items = provider.getItems();

            for (int j = 0 ; j < items.size() ; j++){
                in.succinct.beckn.Item item = items.get(i);
                provider.setItems(new Items());
                provider.getItems().add(item);
                update(provider,true);
            }

        }
        return getReturnIntegrationAdaptor().createStatusResponse(getPath(),null);
    }

}
