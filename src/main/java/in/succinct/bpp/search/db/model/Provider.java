package in.succinct.bpp.search.db.model;

import com.venky.swf.db.annotations.column.UNIQUE_KEY;
import com.venky.swf.db.model.Model;

import java.util.List;
public interface Provider extends Model , IndexedApplicationModel {

    @UNIQUE_KEY
    public String getSubscriberId();
    public void setSubscriberId(String subscriberId);

    List<ProviderLocation> getProviderLocations();
    List<Fulfillment> getFulfillments();
    List<Payment> getPayments();
    List<Category> getCategories();
    List<Item> getItems();
}
