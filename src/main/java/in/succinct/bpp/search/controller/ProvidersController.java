package in.succinct.bpp.search.controller;

import com.venky.cache.Cache;
import com.venky.swf.controller.ModelController;
import com.venky.swf.controller.annotations.RequireLogin;
import com.venky.swf.db.Database;
import com.venky.swf.db.model.Model;
import com.venky.swf.integration.api.HttpMethod;
import com.venky.swf.path.Path;
import com.venky.swf.routing.Config;
import com.venky.swf.views.View;
import in.succinct.beckn.BecknObject;
import in.succinct.beckn.BecknStrings;
import in.succinct.beckn.Categories;
import in.succinct.beckn.Category;
import in.succinct.beckn.Descriptor;
import in.succinct.beckn.Fulfillment;
import in.succinct.beckn.Fulfillments;
import in.succinct.beckn.Item;
import in.succinct.beckn.Items;
import in.succinct.beckn.Location;
import in.succinct.beckn.Locations;
import in.succinct.beckn.Payment;
import in.succinct.beckn.Payments;
import in.succinct.beckn.Provider;
import in.succinct.beckn.Providers;
import in.succinct.bpp.search.db.model.IndexedActivatableModel;
import in.succinct.bpp.search.db.model.IndexedProviderModel;
import in.succinct.bpp.search.db.model.ProviderLocation;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

public class ProvidersController extends ModelController<in.succinct.bpp.search.db.model.Provider> {
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
            ensureProvider(provider,active);
            return getReturnIntegrationAdaptor().createStatusResponse(getPath(),null);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @RequireLogin(false)
    public View ingest() throws Exception{
        ensureIntegrationMethod(HttpMethod.POST);

        Providers providers = new Providers((JSONArray) JSONValue.parse(new InputStreamReader(getPath().getInputStream())));
        for (int i = 0 ; i < providers.size() ; i ++){
            ensureProvider(providers.get(i),true);
        }
        return getReturnIntegrationAdaptor().createStatusResponse(getPath(),null);
    }
    private <T extends Model & IndexedProviderModel> Cache<String,T> createDbCache(Class<T> clazz){
        return new Cache<String,T>(0,0){

            @Override
            protected T getValue(String id) {
                T c = Database.getTable(clazz).newRecord();
                c.setApplicationId(getPath().getApplication().getId());
                c.setObjectId(id);
                c = Database.getTable(clazz).getRefreshed(c);
                if (c.getRawRecord().isNewRecord()){
                    c.setObjectName(id);
                    c.save();
                }
                return c;
            }
        };
    }

    public void ensureProvider(Provider bProvider, boolean active){

        Config.instance().getLogger(getClass().getName()).info("ProvidersController: items size: " + bProvider.getItems().size());
        Items items = bProvider.getItems();bProvider.rm("items");
        Categories categories = bProvider.getCategories();bProvider.rm("categories");
        Fulfillments fulfillments = bProvider.getFulfillments();bProvider.rm("fulfillments");
        Payments payments = bProvider.getPayments();bProvider.rm("payments");
        Locations locations = bProvider.getLocations();bProvider.rm("locations");


        in.succinct.bpp.search.db.model.Provider provider = Database.getTable(in.succinct.bpp.search.db.model.Provider.class).newRecord();
        provider.setApplicationId(getPath().getApplication().getId());
        provider.setObjectId(bProvider.getId());
        provider.setObjectName(bProvider.getDescriptor().getName());
        provider.setObjectJson(bProvider.toString());
        provider = Database.getTable(in.succinct.bpp.search.db.model.Provider.class).getRefreshed(provider);
        provider.save();

        Map<String, in.succinct.bpp.search.db.model.Item> itemMap = createDbCache(in.succinct.bpp.search.db.model.Item.class);
        Map<String, in.succinct.bpp.search.db.model.Category> categoryMap = createDbCache(in.succinct.bpp.search.db.model.Category.class);
        Map<String, in.succinct.bpp.search.db.model.Fulfillment> fulfillmentMap = createDbCache(in.succinct.bpp.search.db.model.Fulfillment.class);
        Map<String, in.succinct.bpp.search.db.model.Payment> paymentMap = createDbCache(in.succinct.bpp.search.db.model.Payment.class);
        Map<String, in.succinct.bpp.search.db.model.ProviderLocation> providerLocationMap = createDbCache(in.succinct.bpp.search.db.model.ProviderLocation.class);


        if (categories != null){
            for (int j = 0 ; j < categories.size() ; j++){
                Category category = categories.get(j);
                in.succinct.bpp.search.db.model.Category model = ensureProviderModel(in.succinct.bpp.search.db.model.Category.class,provider,active,category);
                categoryMap.put(model.getObjectId(),model);
            }
        }
        if (fulfillments != null) {
            for (int j = 0; j < fulfillments.size(); j++) {
                Fulfillment fulfillment = fulfillments.get(j);
                in.succinct.bpp.search.db.model.Fulfillment model = ensureProviderModel(in.succinct.bpp.search.db.model.Fulfillment.class, provider, active, fulfillment);
                fulfillmentMap.put(model.getObjectId(), model);
            }
        }

        if (payments != null) {
            for (int j = 0; j < payments.size(); j++) {
                Payment payment = payments.get(j);
                in.succinct.bpp.search.db.model.Payment model = ensurePayment(provider, payment);
                paymentMap.put(model.getObjectId(), model);
            }
        }

        if (locations != null) {
            for (int j = 0; j < locations.size(); j++) {
                Location location = locations.get(j);
                ProviderLocation model = ensureProviderModel(ProviderLocation.class, provider, active, location);
                providerLocationMap.put(model.getObjectId(), model);
            }
        }

        if (items != null) {
            for (int j = 0; j < items.size(); j++) {
                Item item = items.get(j);
                
                if (item.getCategoryIds() == null){
                    item.setCategoryIds(new BecknStrings());
                }
                if (item.getFulfillmentIds() == null){
                    item.setFulfillmentIds(new BecknStrings());
                }
                if (item.getLocationIds() == null){
                    item.setLocationIds(new BecknStrings());
                }
                if (item.getPaymentIds() == null){
                    item.setPaymentIds(new BecknStrings());
                }

                //for (String categoryId : item.getCategoryIds()) {
                    //for (String locationId : item.getLocationIds()) {
                        //for (String paymentId : item.getPaymentIds()) {
                            //for (String fulfillmentId : item.getFulfillmentIds()) {
                                ensureProviderModel(in.succinct.bpp.search.db.model.Item.class, provider, active, item, (model, becknObject) -> {
                                    //model.setCategoryId(categoryMap.get(categoryId).getId());
                                    model.setCategoryId(1L);

                                    //model.setProviderLocationId(item.get(locationId).getId());
                                    model.setProviderLocationId(1L);

                                    //model.setPaymentId(paymentMap.get(paymentId).getId());
                                    model.setPaymentId(1L);
                                    //model.setFulfillmentId(fulfillmentMap.get(fulfillmentId).getId());
                                    model.setFulfillmentId(1L);
                                });
                            //}
                        //}
                    //}
                //}

            }

        }


    }
    private in.succinct.bpp.search.db.model.Payment ensurePayment(in.succinct.bpp.search.db.model.Provider provider, Payment bPayment) {
        in.succinct.bpp.search.db.model.Payment payment =  Database.getTable(in.succinct.bpp.search.db.model.Payment.class).newRecord();
        payment.setApplicationId(provider.getApplicationId());
        payment.setProviderId(provider.getId());
        payment.setObjectId(bPayment.getId());
        payment.setObjectName(bPayment.getType());
        payment.setObjectJson(bPayment.toString());
        payment = Database.getTable(in.succinct.bpp.search.db.model.Payment.class).getRefreshed(payment);
        payment.save();
        return payment;
    }

    private <T extends Model & IndexedProviderModel>  T ensureProviderModel(Class<T> modelClass, in.succinct.bpp.search.db.model.Provider provider, boolean active, BecknObject becknObject){
        return ensureProviderModel(modelClass,provider,active,becknObject,null);
    }
    private <T extends Model & IndexedProviderModel, B extends BecknObject>  T ensureProviderModel(Class<T> modelClass, in.succinct.bpp.search.db.model.Provider provider, boolean active,
                                                                                                      B becknObject, Visitor<T,B> visitor){
        T model =  Database.getTable(modelClass).newRecord();
        model.setApplicationId(provider.getApplicationId());
        model.setProviderId(provider.getId());
        model.setObjectId(becknObject.get("id"));
        Descriptor descriptor = becknObject.get(Descriptor.class,"descriptor");
        if (descriptor != null) {
            model.setObjectName(descriptor.getName());
        }
        model.setObjectJson(becknObject.toString());
        if (model instanceof IndexedActivatableModel){
            ((IndexedActivatableModel)model).setActive(active);
        }
        model = Database.getTable(modelClass).getRefreshed(model);
        if (visitor != null){
            visitor.visit(model,becknObject);
        }
        Config.instance().getLogger(getClass().getName()).info("ProvidersController: model saved: " + model.getRawRecord().toString());
        model.save();
        return model;
    }

    private interface Visitor<M extends Model & IndexedProviderModel,B extends BecknObject> {
        public void visit(M model , B becknObject);
    }

}
