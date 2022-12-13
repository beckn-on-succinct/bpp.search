package in.succinct.bpp.search.db.model;

import com.venky.swf.db.annotations.column.COLUMN_SIZE;
import com.venky.swf.db.annotations.column.indexing.Index;
import com.venky.swf.db.model.Model;

public interface Provider extends Model {
    @COLUMN_SIZE(1024*6)
    @Index
    public String getProviderJson();
    public void setProviderJson(String itemJson);

    @Index
    public String getItemName();
    public void setItemName(String itemName);

    @Index
    public String getProviderName();
    public void setProviderName(String providerName);

    @Index
    public String getProviderLocationName();
    public void setProviderLocationName(String providerLocationName);

    @Index
    public String getItemCategoryName();
    public void setItemCategoryName(String itemCategoryName);

    @Index
    public String getBppId();
    public void setBppId(String bppId);


    @Index
    public boolean isActive();
    public void setActive(boolean active);



}
