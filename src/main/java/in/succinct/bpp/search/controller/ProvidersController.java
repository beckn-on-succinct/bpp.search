package in.succinct.bpp.search.controller;

import com.venky.cache.Cache;
import com.venky.core.util.ObjectUtil;
import com.venky.swf.controller.ModelController;
import com.venky.swf.controller.annotations.RequireLogin;
import com.venky.swf.db.Database;
import com.venky.swf.db.model.Model;
import com.venky.swf.integration.api.HttpMethod;
import com.venky.swf.path.Path;
import com.venky.swf.routing.Config;
import com.venky.swf.views.View;
import in.succinct.beckn.BecknObject;
import in.succinct.beckn.BecknObjectWithId;
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
import org.json.simple.JSONAware;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
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
    private <T extends Model & IndexedProviderModel> Cache<String,T> createDbCache(Class<T> clazz, long providerId){
        return new Cache<String,T>(0,0){

            @Override
            protected T getValue(String id) {
                T c = Database.getTable(clazz).newRecord();
                c.setApplicationId(getPath().getApplication().getId());
                c.setObjectId(id);
                c.setProviderId(providerId);
                if (!ObjectUtil.isVoid(id)){
                    c = Database.getTable(clazz).getRefreshed(c);
                    if (c.getRawRecord().isNewRecord()){
                        c.setObjectName(id);
                        c.save();
                    }
                }
                return c;
            }
        };
    }
    private static Map<String,Class<? extends Model>> modelClassMap = new HashMap<>(){{
        put("category_ids", in.succinct.bpp.search.db.model.Category.class);
        put("fulfillment_ids", in.succinct.bpp.search.db.model.Fulfillment.class);
        put("location_ids",ProviderLocation.class);
        put("payment_ids", in.succinct.bpp.search.db.model.Payment.class);
    }};
    @SuppressWarnings("unchecked")
    public <T extends Model & IndexedProviderModel> Class<T> getModelClass(String name){
        return (Class<T>)modelClassMap.get(name);
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

        Map<String, in.succinct.bpp.search.db.model.Item> itemMap = createDbCache(in.succinct.bpp.search.db.model.Item.class,provider.getId());
        Map<String, in.succinct.bpp.search.db.model.Category> categoryMap = createDbCache(in.succinct.bpp.search.db.model.Category.class,provider.getId());
        Map<String, in.succinct.bpp.search.db.model.Fulfillment> fulfillmentMap = createDbCache(in.succinct.bpp.search.db.model.Fulfillment.class,provider.getId());
        Map<String, in.succinct.bpp.search.db.model.Payment> paymentMap = createDbCache(in.succinct.bpp.search.db.model.Payment.class,provider.getId());
        Map<String, in.succinct.bpp.search.db.model.ProviderLocation> providerLocationMap = createDbCache(in.succinct.bpp.search.db.model.ProviderLocation.class,provider.getId());



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
                in.succinct.bpp.search.db.model.Fulfillment model = ensureProviderModel(in.succinct.bpp.search.db.model.Fulfillment.class, provider, active, fulfillment,(fulfillmentModel, fulfillmentBecknObject) -> {
                    fulfillmentModel.setObjectName(fulfillmentBecknObject.getType().toString());
                });
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
                ProviderLocation model = ensureProviderModel(ProviderLocation.class, provider, active, location,(pl, l) -> {
                    pl.setObjectName(l.getDescriptor().getName());
                });
                providerLocationMap.put(model.getObjectId(), model);
            }
        }


        if (items != null) {
            for (int j = 0; j < items.size(); j++) {
                Item item = items.get(j);

                for (String key : new String[]{"category_ids","fulfillment_ids","location_ids","payment_ids"}){
                    BecknStrings refIds = item.get(key) == null ? null : new BecknStrings(item.get(key));
                    if (refIds == null){
                        refIds = new BecknStrings();
                        item.set(key,refIds);
                        String singular = key.substring(0,key.length()-1);
                        String refObjectId = item.get(singular);
                        if (refObjectId != null){
                            refIds.add(refObjectId);
                            BecknObjectWithId becknObject = new BecknObjectWithId();
                            becknObject.setId(refObjectId);
                            ensureProviderModel(getModelClass(key),provider,active,becknObject);
                        }
                    }
                    if (refIds.size() == 0){
                        refIds.add(null);
                    }
                }
                for (String categoryId : item.getCategoryIds()) {
                    for (String locationId : item.getLocationIds()) {
                        for (String paymentId : item.getPaymentIds()) {
                            for (String fulfillmentId : item.getFulfillmentIds()) {
                                ensureProviderModel(in.succinct.bpp.search.db.model.Item.class, provider, active, item, (model, becknObject) -> {
                                    if (categoryId != null ) model.setCategoryId(categoryMap.get(categoryId).getId());
                                    if (locationId != null ) model.setProviderLocationId(providerLocationMap.get(locationId).getId());
                                    if (paymentId != null) model.setPaymentId(paymentMap.get(paymentId).getId());
                                    if (fulfillmentId !=null) model.setFulfillmentId(fulfillmentMap.get(fulfillmentId).getId());
                                });
                            }
                        }
                    }
                }
            }
        }

    }
    private in.succinct.bpp.search.db.model.Payment ensurePayment(in.succinct.bpp.search.db.model.Provider provider, Payment bPayment) {
        in.succinct.bpp.search.db.model.Payment payment =  Database.getTable(in.succinct.bpp.search.db.model.Payment.class).newRecord();
        payment.setApplicationId(provider.getApplicationId());
        payment.setProviderId(provider.getId());
        payment.setObjectId(bPayment.getId());
        payment.setObjectName(bPayment.getType().toString());
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
        if (visitor != null){
            visitor.visit(model,becknObject);
        }
        Config.instance().getLogger(getClass().getName()).info("ProvidersController: model saved: " + model.getRawRecord().toString());
        model = Database.getTable(modelClass).getRefreshed(model);
        model.save();
        return model;
    }

    private interface Visitor<M extends Model & IndexedProviderModel,B extends BecknObject> {
        public void visit(M model , B becknObject);
    }

}
