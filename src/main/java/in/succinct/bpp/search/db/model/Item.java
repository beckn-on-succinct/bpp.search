package in.succinct.bpp.search.db.model;

import com.venky.swf.db.annotations.column.indexing.Index;
import com.venky.swf.db.model.Model;

public interface Item extends Model,IndexedProviderModel {
    public Boolean isActive();
    public void setActive(Boolean active);

    @Index
    public Long getCategoryId();
    public void setCategoryId(Long id);
    public Category getCategory();

    @Index
    public Long getFulfillmentId();
    public void setFulfillmentId(Long id);
    public Fulfillment getFulfillment();

    @Index
    public Long getPaymentId();
    public void setPaymentId(Long id);
    public Payment getPayment();

    @Index
    public Long getProviderLocationId();
    public void setProviderLocationId(Long id);
    public ProviderLocation getProviderLocation();


}
