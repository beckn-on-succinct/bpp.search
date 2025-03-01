package in.succinct.bpp.search.adaptor;

import com.venky.cache.Cache;
import com.venky.core.date.DateUtils;
import com.venky.core.util.Bucket;
import com.venky.core.util.ObjectUtil;
import com.venky.swf.db.Database;
import com.venky.swf.db.model.Model;
import com.venky.swf.plugins.beckn.messaging.Subscriber;
import com.venky.swf.plugins.collab.db.model.config.City;
import com.venky.swf.plugins.lucene.index.LuceneIndexer;
import com.venky.swf.routing.Config;
import com.venky.swf.sql.Conjunction;
import com.venky.swf.sql.Expression;
import com.venky.swf.sql.Operator;
import com.venky.swf.sql.Select;
import in.succinct.beckn.BecknStrings;
import in.succinct.beckn.BreakUp;
import in.succinct.beckn.BreakUp.BreakUpElement;
import in.succinct.beckn.BreakUp.BreakUpElement.BreakUpCategory;
import in.succinct.beckn.Catalog;
import in.succinct.beckn.Categories;
import in.succinct.beckn.Category;
import in.succinct.beckn.Context;
import in.succinct.beckn.Descriptor;
import in.succinct.beckn.Error;
import in.succinct.beckn.Error.Type;
import in.succinct.beckn.Fulfillment.RetailFulfillmentType;
import in.succinct.beckn.FulfillmentStop;
import in.succinct.beckn.Fulfillments;
import in.succinct.beckn.Images;
import in.succinct.beckn.Intent;
import in.succinct.beckn.Item;
import in.succinct.beckn.ItemQuantity;
import in.succinct.beckn.Items;
import in.succinct.beckn.Location;
import in.succinct.beckn.Locations;
import in.succinct.beckn.Message;
import in.succinct.beckn.Order;
import in.succinct.beckn.Order.NonUniqueItems;
import in.succinct.beckn.Payments;
import in.succinct.beckn.Price;
import in.succinct.beckn.Provider;
import in.succinct.beckn.Providers;
import in.succinct.beckn.Quantity;
import in.succinct.beckn.Quote;
import in.succinct.beckn.Request;
import in.succinct.beckn.SellerException;
import in.succinct.beckn.SellerException.ItemQuantityUnavailable;
import in.succinct.beckn.Time;
import in.succinct.bpp.core.adaptor.AbstractCommerceAdaptor;
import in.succinct.bpp.core.db.model.ProviderConfig.Serviceability;
import in.succinct.bpp.search.db.model.IncrementalSearchRequest;
import in.succinct.catalog.indexer.db.model.Fulfillment;
import in.succinct.catalog.indexer.db.model.IndexedSubscriberModel;
import in.succinct.catalog.indexer.db.model.Payment;
import in.succinct.catalog.indexer.db.model.ProviderLocation;
import in.succinct.json.JSONAwareWrapper;
import org.apache.lucene.search.Query;
import org.json.simple.JSONObject;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

public abstract class SearchAdaptor extends AbstractCommerceAdaptor {

    public SearchAdaptor(Map<String, String> configuration, Subscriber subscriber) {
        super(configuration, subscriber);
    }

    public void search(Request request, Request reply) {
        try{
            indexed_search(request,reply);
        }catch (Exception ex){
            Config.instance().getLogger(getClass().getName()).log(Level.WARNING,"Exception found", ex);
        }
    }

    private void indexed_search(Request request, Request reply) {
        //request.getContext().
        reply.setMessage(new Message());
        Catalog catalog = new Catalog();
        catalog.setDescriptor(new Descriptor());
        Subscriber subscriber = getSubscriber();
        catalog.getDescriptor().setName(getProviderConfig().getStoreName());
        catalog.getDescriptor().setLongDesc(getProviderConfig().getStoreName());
        catalog.getDescriptor().setShortDesc(getProviderConfig().getStoreName());
        catalog.getDescriptor().setCode(subscriber.getSubscriberId());
        catalog.getDescriptor().setImages(new Images());
        catalog.getDescriptor().setSymbol(getProviderConfig().getLogo());
        catalog.getDescriptor().getImages().add(getProviderConfig().getLogo());

        reply.getMessage().setCatalog(catalog);
        Providers providers = new Providers();
        catalog.setProviders(providers);
        catalog.setFulfillments(new Fulfillments());

        Message message  = request.getMessage();
        Intent intent = message.getIntent();
        if (intent.isIncrementalRequestStartTrigger()){
            intent.setStartTime(request.getContext().getTimestamp());
        }else if (intent.isIncrementalRequestEndTrigger()){
            intent.setEndTime(request.getContext().getTimestamp());
        }
        IncrementalSearchRequest incrementalSearchRequest = null;
        if (intent.isIncrementalRequest()){
            Context context =request.getContext() ;
            incrementalSearchRequest = Database.getTable(IncrementalSearchRequest.class).newRecord();
            incrementalSearchRequest.setBecknTransactionId(context.getTransactionId());
            if (intent.getStartTime() != null) {
                incrementalSearchRequest.setStartTime(new Timestamp(intent.getStartTime().getTime()));
            }
            if (intent.getEndTime() != null) {
                incrementalSearchRequest.setEndTime(new Timestamp(intent.getEndTime().getTime()));
            }
            incrementalSearchRequest.setNetworkId(context.getNetworkId());
            incrementalSearchRequest.setSubscriberJson(getSubscriber().getInner().toString());
            JSONObject properties = new JSONObject();
            this.getConfiguration().forEach((k,v)->{
                if (k.startsWith("in.succinct.bpp")) {
                    properties.put(k, v);
                }
            });

            JSONObject headers = request.getExtendedAttributes().get("headers");

            incrementalSearchRequest.setAppId(subscriber.getAppId());
            incrementalSearchRequest.setHeaders(headers.toString());
            incrementalSearchRequest.setCommerceAdaptorProperties(properties.toString());
            incrementalSearchRequest = Database.getTable(IncrementalSearchRequest.class).getRefreshed(incrementalSearchRequest);
            if (incrementalSearchRequest.getRawRecord().isNewRecord()){
                incrementalSearchRequest.setRequestPayload(request.toString());
            }
            incrementalSearchRequest.save();
        }
        in.succinct.beckn.Fulfillment intentFulfillment = intent.getFulfillment();
        if (intentFulfillment != null){
            RetailFulfillmentType type = intentFulfillment.getType() != null ? RetailFulfillmentType.valueOf(intentFulfillment.getType()) : null;
            if (type == RetailFulfillmentType.home_delivery && !getProviderConfig().isHomeDeliverySupported()){
                return;
            }else if (type == RetailFulfillmentType.store_pickup && !getProviderConfig().isStorePickupSupported()){
                return;
            }else if (type == RetailFulfillmentType.return_to_origin && (!getProviderConfig().isReturnPickupSupported() || !getProviderConfig().isReturnSupported())){
                return;
            }
        }
        Descriptor intentDescriptor = normalizeDescriptor(intent.getDescriptor()) ;

        Provider provider = intent.getProvider();
        Descriptor providerDescriptor = provider == null ? intentDescriptor : normalizeDescriptor(provider.getDescriptor());



        Item item = intent.getItem();
        Descriptor itemDescriptor = item == null ? intentDescriptor : normalizeDescriptor(item.getDescriptor());

        Category category = intent.getCategory();
        Descriptor categoryDescriptor = category == null ? intentDescriptor : normalizeDescriptor(category.getDescriptor());

        StringBuilder q = new StringBuilder();
        if (providerDescriptor != null && !ObjectUtil.isVoid(providerDescriptor.getName())){
            providerDescriptor.setName(providerDescriptor.getName().trim());
            q.append(String.format("     ( PROVIDER:%s* or PROVIDER_LOCATION:%s* )",providerDescriptor.getName(),providerDescriptor.getName()));
        }else if (provider != null && !ObjectUtil.isVoid(provider.getId())) {
            in.succinct.catalog.indexer.db.model.Provider dbProvider = Database.getTable(in.succinct.catalog.indexer.db.model.Provider.class).newRecord();
            dbProvider.setSubscriberId(getSubscriber().getSubscriberId());
            dbProvider.setObjectId(provider.getId());
            dbProvider = Database.getTable(in.succinct.catalog.indexer.db.model.Provider.class).getRefreshed(dbProvider);
            if (dbProvider.getRawRecord().isNewRecord()){
                q.append(String.format(" ( PROVIDER:%s or PROVIDER_LOCATION:%s )", provider.getId(), provider.getId()));
            }else {
                q.append(String.format(" ( PROVIDER_ID:%d or PROVIDER_LOCATION_ID:%d )", dbProvider.getId(), dbProvider.getId()));
            }
        }
        if (categoryDescriptor != null && !ObjectUtil.isVoid(categoryDescriptor.getName())){
            categoryDescriptor.setName(categoryDescriptor.getName().trim());
            if (q.length() > 0){
                q.append( intentDescriptor == null ? " AND " : " OR " );
            }
            q.append(String.format(" CATEGORY:%s* ",categoryDescriptor.getName()));
        }else if (category != null && !ObjectUtil.isVoid(category.getId())){
            if (q.length() > 0){
                q.append( intentDescriptor == null ? " AND " : " OR " );
            }
            category.setId(category.getId().trim());
            q.append(String.format(" CATEGORY:%s* ",category.getId()));
        }
        if (itemDescriptor != null && !ObjectUtil.isVoid(itemDescriptor.getName())){
            itemDescriptor.setName(itemDescriptor.getName().trim());
            if (q.length() > 0){
                q.append( intentDescriptor == null ? " AND " : " OR " );
            }
            q.append(String.format(" OBJECT_NAME:%s* ",itemDescriptor.getName()));
        }
        List<Long> itemIds = new ArrayList<>();
        if (!ObjectUtil.isVoid(q.toString())) {
            LuceneIndexer indexer = LuceneIndexer.instance(in.succinct.catalog.indexer.db.model.Item.class);
            Query query = indexer.constructQuery(q.toString());
            Config.instance().getLogger(getClass().getName()).info("Searching for /items/search/" + q);
            itemIds = indexer.findIds(query, 0);
            Config.instance().getLogger(getClass().getName()).info("SearchAdaptor: Query result size: " + itemIds.size());
            if (itemIds.isEmpty()) {
                reply.setSuppressed(true);
                return;
                // Empty provider list.
            }
        }


        Select sel = new Select().from(in.succinct.catalog.indexer.db.model.Item.class);
        Expression where = new Expression(sel.getPool(), Conjunction.AND);
        if (incrementalSearchRequest == null ) {
            where.add(new Expression(sel.getPool(), "ACTIVE", Operator.EQ, true));
        }
        where.add(new Expression(sel.getPool(),"SUBSCRIBER_ID", Operator.EQ, getSubscriber().getSubscriberId()));
        if (incrementalSearchRequest != null) {
            if (incrementalSearchRequest.getLastTransmissionTime() == null) {
                incrementalSearchRequest.setLastTransmissionTime(incrementalSearchRequest.getStartTime());
            }
            if (incrementalSearchRequest.getEndTime() != null) {
                if (incrementalSearchRequest.getLastTransmissionTime().after(incrementalSearchRequest.getEndTime())) {
                    itemIds.clear();
                    itemIds.add(-1L);
                }
            }
            where.add(new Expression(sel.getPool(), "UPDATED_AT", Operator.GT, incrementalSearchRequest.getLastTransmissionTime()));
            if (incrementalSearchRequest.getEndTime() != null) {
                where.add(new Expression(sel.getPool(), "UPDATED_AT", Operator.LE, incrementalSearchRequest.getEndTime()));
            }
            incrementalSearchRequest.setLastTransmissionTime(new Timestamp(System.currentTimeMillis()));
            incrementalSearchRequest.save();
        }

        if (!itemIds.isEmpty()){
            where.add(Expression.createExpression(sel.getPool(),"ID",Operator.IN,itemIds.toArray()));
        }

        sel.where(where).add(String.format(" and provider_location_id in ( select id from provider_locations where provider_id in (select id from providers where subscriber_id = '%s'))",
                 getSubscriber().getSubscriberId())     );

        List<in.succinct.catalog.indexer.db.model.Item> records = sel.where(where).execute(in.succinct.catalog.indexer.db.model.Item.class, 30);

        Bucket numItemsReturned = new Bucket();
        Set<String> subscriberIds = new HashSet<>();
        Set<Long> providerIds = new HashSet<>();
        Set<Long> providerLocationIds = new HashSet<>();
        Set<Long> fulfillmentIds = new HashSet<>();
        Set<Long> categoryIds = new HashSet<>();
        Set<Long> paymentIds = new HashSet<>();

        Cache<String, Cache<Long, in.succinct.catalog.indexer.db.model.Item>> appItemMap = createAppDbCache(in.succinct.catalog.indexer.db.model.Item.class,new HashSet<>());
        records.forEach(i->{
            subscriberIds.add(i.getSubscriberId());
            providerIds.add(i.getProviderId());
            providerLocationIds.addAll(BecknStrings.parse(i.getLocationIds()));
            fulfillmentIds.addAll(BecknStrings.parse(i.getFulfillmentIds()));
            categoryIds.addAll(BecknStrings.parse(i.getCategoryIds()));
            paymentIds.addAll(BecknStrings.parse(i.getPaymentIds()));
            appItemMap.get(i.getSubscriberId()).put(i.getId(),i);
        });
        Cache<String, Cache<Long, in.succinct.catalog.indexer.db.model.Provider>> appProviderMap = createAppDbCache(in.succinct.catalog.indexer.db.model.Provider.class,providerIds);
        Cache<String, Cache<Long, in.succinct.catalog.indexer.db.model.ProviderLocation>> appLocationMap = createAppDbCache(in.succinct.catalog.indexer.db.model.ProviderLocation.class,providerLocationIds);
        Cache<String, Cache<Long, in.succinct.catalog.indexer.db.model.Fulfillment>> appFulfillmentMap = createAppDbCache(in.succinct.catalog.indexer.db.model.Fulfillment.class,fulfillmentIds);
        Cache<String, Cache<Long, in.succinct.catalog.indexer.db.model.Category>> appCategoryMap = createAppDbCache(in.succinct.catalog.indexer.db.model.Category.class,categoryIds);
        Cache<String, Cache<Long, in.succinct.catalog.indexer.db.model.Payment>> appPaymentMap = createAppDbCache(in.succinct.catalog.indexer.db.model.Payment.class,paymentIds);



        for (String subscriberId : subscriberIds) {

            Map<Long, in.succinct.catalog.indexer.db.model.Item> itemMap = appItemMap.get(subscriberId);
            for (Long itemId : itemMap.keySet()) {
                Config.instance().getLogger(getClass().getName()).info("SearchAdaptor: Looping through result items" + itemId);
                in.succinct.catalog.indexer.db.model.Item dbItem = itemMap.get(itemId);

                in.succinct.catalog.indexer.db.model.Provider dbProvider = appProviderMap.get(subscriberId).get(dbItem.getProviderId());

                Provider outProvider = providers.get(dbProvider.getObjectId());
                if (outProvider == null) {
                    outProvider = new Provider(dbProvider.getObjectJson());
                    outProvider.setExp(DateUtils.addMinutes(request.getContext().getTimestamp(),Database.getJdbcTypeHelper("").getTypeRef(int.class).getTypeConverter().valueOf(outProvider.getTtl())));
                    /* Only on Beckn 1.1
                    TagGroup tagGroup = new TagGroup();
                    outProvider.setTags(tagGroup);
                    tagGroup.setCode("Context");
                    tagGroup.setName("Context");
                    tagGroup.setDisplay(true);
                    Tags tags = new Tags();
                    tagGroup.setList(tags);
                    Tag tag = new Tag();
                    tags.add(tag);
                    tag.setCode("bpp_id");
                    tag.setName("bpp_id");
                    tag.setDisplay(true);
                    tag.setValue(application.getAppId());
                    outProvider.setBppId(application.getAppId());
                    */
                    if (incrementalSearchRequest == null) {
                        Time time = new Time();
                        time.setLabel("enable");
                        time.setTimestamp(reply.getContext().getTimestamp());
                        outProvider.setTime(time);
                    }

                    providers.add(outProvider);

                }
                Categories categories = outProvider.getCategories();
                if (categories == null) {
                    categories = new Categories(){{
                        for (in.succinct.catalog.indexer.db.model.Category dbProviderCategory : dbProvider.getCategories()) {
                            add(new Category(dbProviderCategory.getObjectJson()));
                        }
                    }};
                    outProvider.setCategories(categories);
                }

                Locations locations = outProvider.getLocations();
                if (locations == null) {
                    locations = new Locations(){{
                        for (ProviderLocation providerLocation : dbProvider.getProviderLocations()) {
                            add(new Location(providerLocation.getObjectJson()));
                        }
                    }};
                    outProvider.setLocations(locations);
                }

                Fulfillments fulfillments = outProvider.getFulfillments();
                if (fulfillments == null) {
                    fulfillments = new Fulfillments() {{
                        for (Fulfillment fulfillment : dbProvider.getFulfillments()) {
                            add(new in.succinct.beckn.Fulfillment(fulfillment.getObjectJson()));
                        }
                    }};
                    outProvider.setFulfillments(fulfillments);
                    catalog.setFulfillments(fulfillments);
                }
                
                Payments payments = outProvider.getPayments();
                if (payments == null) {
                    in.succinct.beckn.Payment bapPaymentIntent = request.getMessage().getIntent().getPayment();
                    payments = new Payments(){{
                        for (Payment payment : dbProvider.getPayments()) {
                            in.succinct.beckn.Payment becknPayment = new in.succinct.beckn.Payment(payment.getObjectJson());
                            add(becknPayment);
                            if (bapPaymentIntent != null ) {
                                becknPayment.update(bapPaymentIntent); //need toupdate from the intent.!
                            }
                            
                        }
                    }};
                    outProvider.setPayments(payments);
                }
                Items items = outProvider.getItems();
                if (items == null) {
                    items = new Items();
                    outProvider.setItems(items);
                }
                if (items.get(dbItem.getObjectId()) == null) {
                    Item outItem = new Item((JSONObject) JSONAwareWrapper.parse(dbItem.getObjectJson()));
                    outItem.setTime(new Time());
                    outItem.getTime().setLabel(dbItem.isActive() ? "enable" : "disable");
                    outItem.getTime().setTimestamp(dbItem.getUpdatedAt());
                    String outFulfillmentType = fulfillments.get(outItem.getFulfillmentId()).getType() ;
                    String inFulfillmentType = intentFulfillment == null || intentFulfillment.getType() == null ?  null : intentFulfillment.getType();

                    FulfillmentStop end = intentFulfillment == null ? null : intentFulfillment._getEnd();


                    if (outFulfillmentType != null && outFulfillmentType.matches(inFulfillmentType) ) {

                        outItem.setMatched(true);
                        outItem.setRelated(true);
                        outItem.setRecommended(true);

                        ItemQuantity itemQuantity = new ItemQuantity();
                        Quantity available =new Quantity() ;
                        available.setCount(getProviderConfig().getMaxOrderQuantity()); // Ordering more than 20 is not allowed.
                        itemQuantity.setAvailable(available);
                        itemQuantity.setMaximum(available);

                        outItem.setTentativeItemQuantity(itemQuantity);

                        Location storeLocation = locations.get(outItem.getLocationId());
                        City city = City.findByCountryAndStateAndName(storeLocation.getCountry().getCode(),storeLocation.getState().getCode(),storeLocation.getCity().getCode());

                        boolean storeInCity = ObjectUtil.equals(city.getCode(),request.getContext().getCity()) || ObjectUtil.equals(request.getContext().getCity(),"*");

                        if (end != null && getProviderConfig().getServiceability(inFulfillmentType,end,storeLocation).isServiceable()){
                            items.add(outItem);
                            numItemsReturned.increment();
                        }else if (end == null && storeInCity) {
                            items.add(outItem);
                            numItemsReturned.increment();
                        }
                    }
                }

            }
        }
        if (incrementalSearchRequest != null){
            catalog.setFulfillments(null);
            catalog.setDescriptor(null);
        }
        reply.setSuppressed(numItemsReturned.intValue() == 0); // No need to send on_search back!T

    }

    private Descriptor normalizeDescriptor(Descriptor descriptor) {
        if (descriptor != null && descriptor.getInner().isEmpty()){
            descriptor = null;
        }
        return descriptor;
    }

    private <T extends Model> Cache<Long,T> createDbCache(Class<T> clazz, Set<Long> ids) {
        Cache<Long,T> cache = new Cache<>(0,0){

            @Override
            protected T getValue(Long id) {
                if (id == null){
                    return null;
                }else {
                    return Database.getTable(clazz).get(id);
                }
            }
        };
        if (!ids.isEmpty() && !ids.contains(null)){
            Select select = new Select().from(clazz);
            select.where(new Expression(select.getPool(),"ID",Operator.IN,ids.toArray()));
            select.execute(clazz).forEach(t->cache.put(t.getId(),t));
        }
        return cache;
    }

    private <T extends Model & IndexedSubscriberModel> Cache<String,Cache<Long,T>> createAppDbCache(Class<T> clazz, Set<Long> ids){
        Cache<String,Cache<Long,T>> cache = new Cache<>(0,0) {
            @Override
            protected Cache<Long, T> getValue(String subscriberId) {
                return createDbCache(clazz,new HashSet<>());
            }
        };
        if (!ids.isEmpty() && !ids.contains(null)){
            Select select = new Select().from(clazz);
            select.where(new Expression(select.getPool(),"ID",Operator.IN,ids.toArray()));
            select.execute(clazz).forEach(t->cache.get(t.getSubscriberId()).put(t.getId(),t));
        }
        return cache;
    }


    public void select( Request request, Request reply) {
        Error error = new Error();
        reply.setMessage(new Message());

        reply.getMessage().setOrder(getQuote(request.getContext(),request.getMessage().getOrder(),error));
        if (!error.isEmpty()){
            reply.setError(error);
        }
    }

    private Order getQuote(Context context, Order order, Error error) {
        fixFulfillment(context,order);
        in.succinct.beckn.Fulfillments fulfillments  = order.getFulfillments();
        in.succinct.beckn.Fulfillment fulfillment = fulfillments == null || fulfillments.isEmpty() ? null : fulfillments.get(0);
        FulfillmentStop end = fulfillment == null ? null : fulfillment._getEnd();

        fixLocation(order);
        Location  providerLocation = order.getProvider().getLocations().get(0);

        Order finalOrder = new Order();
        finalOrder.setProvider(new Provider());
        finalOrder.getProvider().setId(getSubscriber().getSubscriberId());
        finalOrder.setQuote(new Quote());
        finalOrder.setItems(new NonUniqueItems());
        finalOrder.getProvider().setLocations(new Locations());
        finalOrder.setFulfillments(new Fulfillments());

        Serviceability serviceability = null;
        if (fulfillment != null){
            finalOrder.getFulfillments().add(fulfillment);
            serviceability = getProviderConfig().getServiceability(fulfillment.getType(),end,providerLocation);
            if (serviceability.isServiceable()){
                fulfillment.getState(true).getDescriptor(true).setCode("Serviceable");
            }
        }else {
            for (in.succinct.beckn.Fulfillment f : getFulfillments()){
                serviceability = getProviderConfig().getServiceability(f.getType(),null, providerLocation);
                if (serviceability.isServiceable()){
                    fulfillment = new in.succinct.beckn.Fulfillment(f.toString());
                    fulfillment.getState(true).getDescriptor(true).setCode("Serviceable");
                    finalOrder.getFulfillments().add(fulfillment);
                    break;
                }
            }
        }

        Location finalLocation = new Location();
        finalLocation.setId(providerLocation.getId());
        finalOrder.getProvider().getLocations().add(finalLocation);


        Price orderPrice = new Price();
        orderPrice.setValue(0.0);
        orderPrice.setCurrency(getCurrency());
        BreakUp breakUp = new BreakUp();


        finalOrder.getQuote().setPrice(orderPrice);
        finalOrder.getQuote().setBreakUp(breakUp);


        double deliveryTaxRate = 0.18 ;
        BreakUpElement shipping_total = breakUp.createElement(BreakUpCategory.delivery,"Delivery Charges", new Price());
        shipping_total.getPrice().setCurrency(orderPrice.getCurrency());
        shipping_total.getPrice().setValue(0.0);
        if (serviceability != null && serviceability.isServiceable()){
            shipping_total.getPrice().setValue(serviceability.getCharges());
            breakUp.add(shipping_total);
            shipping_total.setItemId(fulfillment.getId());
        }
        BreakUpElement tax_total = breakUp.createElement(BreakUpCategory.tax,"Tax", new Price());
        tax_total.getPrice().setValue(0.0);
        if (!isTaxIncludedInPrice()) {
            breakUp.add(tax_total);
            if (fulfillment != null){
                tax_total.setItemId(fulfillment.getId());
            }
        }


        NonUniqueItems outItems = finalOrder.getItems();
        NonUniqueItems inItems = order.getItems();

        for (int i = 0 ; i < inItems.size() ; i ++ ){
            Item inItem = inItems.get(i);
            Quantity quantity = inItem.get(Quantity.class,"quantity");


            in.succinct.catalog.indexer.db.model.Item dbItem = getItem(inItem.getId());
            if (dbItem == null ){
                continue;
            }

            Item unitItem = new Item((JSONObject) JSONAwareWrapper.parse(dbItem.getObjectJson()));

            unitItem.setFulfillmentId(finalOrder.getFulfillments() == null || finalOrder.getFulfillments().isEmpty() ? null : finalOrder.getFulfillments().get(0).getId());
            unitItem.setFulfillmentIds(null);

            Quantity avail = new Quantity(); avail.setCount(getProviderConfig().getMaxOrderQuantity());
            if (!dbItem.isActive()){
                avail.setCount(0);
                quantity.setCount(0);
                ItemQuantityUnavailable ex = new SellerException.ItemQuantityUnavailable() ;
                error.setCode(ex.getErrorCode());
                error.setMessage(ex.getMessage());
                error.setType(Type.DOMAIN_ERROR);
            }

            ItemQuantity outQuantity = new ItemQuantity();
            outQuantity.setSelected(quantity);
            outQuantity.setAvailable(avail);
            outQuantity.setMaximum(avail);

            //unitItem.setItemQuantity(outQuantity);
            //0.9.4 was fixed to not send this breakup.
            unitItem.setQuantity(quantity); // Just send the selected quantity.
            unitItem.setTags(null);

            outItems.add(unitItem);

            BreakUpElement element = breakUp.createElement(BreakUpCategory.item, unitItem.getDescriptor().getName(), unitItem.getPrice(), quantity.getCount());
            element.setItemId(unitItem.getId());
            element.setItemQuantity(quantity);

            Item quoteItem = new Item((JSONObject) JSONAwareWrapper.parse(unitItem.getInner().toString()));
            quoteItem.setFulfillmentId(null);
            quoteItem.setItemQuantity(outQuantity);


            //quoteItem.setItemQuantity(null);
            //quoteItem.setQuantity(quantity);
            element.setItem(quoteItem);

            breakUp.add(element);
            orderPrice.setCurrency(getCurrency());
            orderPrice.setValue(orderPrice.getValue() + element.getPrice().getValue());

            Price orderTaxPrice = tax_total.getPrice();
            orderTaxPrice.setCurrency(getCurrency());
            orderTaxPrice.setValue(orderTaxPrice.getValue() + unitItem.getTax().getValue() * quantity.getCount());
        }

        Price orderTaxPrice = tax_total.getPrice();
        orderTaxPrice.setCurrency(getCurrency());
        //Inlude shipping tax
        double factor = isTaxIncludedInPrice() ? deliveryTaxRate/ (1 + deliveryTaxRate) : deliveryTaxRate;
        orderTaxPrice.setValue(orderTaxPrice.getValue() + factor * shipping_total.getPrice().getValue());


        orderPrice.setValue(orderPrice.getValue() + shipping_total.getPrice().getValue()  + (isTaxIncludedInPrice() ? 0 : orderTaxPrice.getValue()));
        orderPrice.setCurrency(getCurrency());


        finalOrder.getQuote().setTtl(15L*60L); //15 minutes.

        return finalOrder;
    }

    private in.succinct.catalog.indexer.db.model.Item getItem(String objectId) {

        Select select = new Select().from(in.succinct.catalog.indexer.db.model.Item.class);
        List<in.succinct.catalog.indexer.db.model.Item> dbItems = select.where(new Expression(select.getPool(), Conjunction.AND).
                add(new Expression(select.getPool(), "SUBSCRIBER_ID", Operator.EQ, getSubscriber().getSubscriberId())).
                add(new Expression(select.getPool(), "OBJECT_ID", Operator.EQ, objectId))).execute(1);

        return dbItems.isEmpty() ? null : dbItems.get(0);
    }




}
