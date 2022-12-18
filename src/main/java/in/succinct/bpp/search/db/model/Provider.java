package in.succinct.bpp.search.db.model;

import com.venky.swf.db.model.Model;

import java.util.List;
public interface Provider extends Model , IndexedApplicationModel {
    List<ProviderLocation> getProviderLocations();
    List<Fulfillment> getFulfillments();
    List<Payment> getPayments();
    List<Category> getCategories();
    List<Item> getItems();
}
