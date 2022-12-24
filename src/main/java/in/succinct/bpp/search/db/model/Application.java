package in.succinct.bpp.search.db.model;

import com.venky.swf.db.model.application.WhiteListIp;

import java.util.List;

public interface Application extends com.venky.swf.plugins.collab.db.model.participants.Application {
    public List<Category> getCategories();
    public List<Fulfillment> getFulfillments();
    public List<Item> getItems();
    public List<Payment> getPayments();
    public List<Provider> getProviders();
    public List<ProviderLocation> getProviderLocations();

}
