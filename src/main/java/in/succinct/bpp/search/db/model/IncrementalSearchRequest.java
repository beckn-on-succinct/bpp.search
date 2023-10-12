package in.succinct.bpp.search.db.model;

import com.venky.swf.db.annotations.column.COLUMN_SIZE;
import com.venky.swf.db.annotations.column.UNIQUE_KEY;
import com.venky.swf.db.annotations.column.validations.Enumeration;
import com.venky.swf.db.model.Model;
import in.succinct.bpp.core.db.model.BecknOrderMeta;

import java.sql.Timestamp;

public interface IncrementalSearchRequest extends Model {
    @UNIQUE_KEY
    public String getBecknTransactionId();
    public void setBecknTransactionId(String becknTransactionId);

    @COLUMN_SIZE(4096)
    public String getSubscriberJson();
    public void setSubscriberJson(String subscriberJson);

    @COLUMN_SIZE(4096)
    public String getHeaders();
    public void setHeaders(String headers);

    @COLUMN_SIZE(4096 * 2 )
    public String getCommerceAdaptorProperties();
    public void setCommerceAdaptorProperties(String commerceAdaptorProperties);

    public String getAppId();
    public void setAppId(String appId);

    public String getNetworkId();
    public void setNetworkId(String networkId);

    @COLUMN_SIZE(4096)
    public String getRequestPayload();
    public void setRequestPayload(String requestPayload);

    public Timestamp getStartTime();
    public void setStartTime(Timestamp startTime);

    public Timestamp getEndTime();
    public void setEndTime(Timestamp endTime);


    public Timestamp getLastTransmissionTime();
    public void setLastTransmissionTime(Timestamp lastTransmissionTime);




}
