package in.succinct.bpp.search.db.model;

import com.venky.swf.db.annotations.column.UNIQUE_KEY;
import com.venky.swf.db.annotations.column.indexing.Index;
import com.venky.swf.db.annotations.model.HAS_DESCRIPTION_FIELD;
import com.venky.swf.db.model.application.Application;

@HAS_DESCRIPTION_FIELD("OBJECT_NAME")
public interface IndexedApplicationModel {
    @UNIQUE_KEY
    @Index
    public Long getApplicationId();
    public void setApplicationId(Long id);
    public Application getApplication();


    @UNIQUE_KEY
    @Index
    public String getObjectId();
    public void setObjectId(String objectId);

    @Index
    public String getObjectName();
    public void setObjectName(String objectName);


    public String getObjectJson();
    public void setObjectJson(String objectJson);
}
