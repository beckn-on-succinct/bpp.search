package in.succinct.bpp.search.db.model;

import com.venky.swf.db.annotations.column.indexing.Index;

public interface IndexedProviderModel extends IndexedApplicationModel {

    @Index
    public Long getProviderId();
    public void setProviderId(Long id);
    public Provider getProvider();



}
