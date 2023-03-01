package in.succinct.bpp.search.adaptor;

import com.venky.cache.Cache;
import com.venky.core.math.DoubleHolder;
import com.venky.swf.db.Database;
import com.venky.swf.db.model.Model;
import com.venky.swf.db.model.application.Application;
import com.venky.swf.plugins.beckn.messaging.Subscriber;
import com.venky.swf.plugins.lucene.index.LuceneIndexer;
import com.venky.swf.routing.Config;
import com.venky.swf.sql.Conjunction;
import com.venky.swf.sql.Expression;
import com.venky.swf.sql.Operator;
import com.venky.swf.sql.Select;
import in.succinct.beckn.BreakUp;
import in.succinct.beckn.BreakUp.BreakUpElement;
import in.succinct.beckn.BreakUp.BreakUpElement.BreakUpCategory;
import in.succinct.beckn.Catalog;
import in.succinct.beckn.Categories;
import in.succinct.beckn.Category;
import in.succinct.beckn.Context;
import in.succinct.beckn.Descriptor;
import in.succinct.beckn.Fulfillment.FulfillmentType;
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
import in.succinct.beckn.Payment.CommissionType;
import in.succinct.beckn.Payments;
import in.succinct.beckn.Price;
import in.succinct.beckn.Provider;
import in.succinct.beckn.Providers;
import in.succinct.beckn.Quantity;
import in.succinct.beckn.QuantitySummary;
import in.succinct.beckn.Quote;
import in.succinct.beckn.Request;
import in.succinct.bpp.core.adaptor.CommerceAdaptor;
import in.succinct.bpp.core.db.model.BecknOrderMeta;
import in.succinct.bpp.core.db.model.ProviderConfig.Serviceability;
import in.succinct.bpp.search.db.model.Fulfillment;
import in.succinct.bpp.search.db.model.IndexedApplicationModel;
import in.succinct.bpp.search.db.model.Payment;
import in.succinct.bpp.search.db.model.ProviderLocation;
import org.apache.lucene.search.Query;
import org.json.simple.JSONArray;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

public class SearchAdaptor {
    final CommerceAdaptor adaptor;

    public SearchAdaptor(CommerceAdaptor adaptor){
        this.adaptor = adaptor;
    }

    public Subscriber getSubscriber() {
        return adaptor.getSubscriber();
    }

    public void search(Request request, Request reply) {
        try{
            _search(request,reply);
        }catch (Exception ex){
            Config.instance().getLogger(getClass().getName()).log(Level.WARNING,"Exception found", ex);
        }
    }

    public void _search(Request request, Request reply) {
        //request.getContext().
        Message message  = request.getMessage();
        Intent intent = message.getIntent();
        in.succinct.beckn.Fulfillment intentFulfillment = intent.getFulfillment();
        if (intentFulfillment != null){
            if (intentFulfillment.getType() == FulfillmentType.home_delivery && !adaptor.getProviderConfig().isHomeDeliverySupported()){
                return;
            }else if (intentFulfillment.getType() == FulfillmentType.store_pickup && !adaptor.getProviderConfig().isStorePickupSupported()){
                return;
            }else if (intentFulfillment.getType() == FulfillmentType.return_to_origin && (!adaptor.getProviderConfig().isReturnPickupSupported() || !adaptor.getProviderConfig().isReturnSupported())){
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
        if (providerDescriptor != null){
            q.append(String.format(" ( PROVIDER:%s* or PROVIDER_LOCATION:%s* )",providerDescriptor.getName(),providerDescriptor.getName()));
        }
        if (categoryDescriptor != null){
            if (q.length() > 0){
                q.append( intentDescriptor == null ? " AND " : " OR " );
            }
            q.append(String.format(" CATEGORY:%s* ",categoryDescriptor.getName()));
        }
        if (itemDescriptor != null){
            if (q.length() > 0){
                q.append( intentDescriptor == null ? " AND " : " OR " );
            }
            q.append(String.format(" OBJECT_NAME:%s* ",itemDescriptor.getName()));
        }


        LuceneIndexer indexer = LuceneIndexer.instance(in.succinct.bpp.search.db.model.Item.class);
        Query query = indexer.constructQuery(q.toString());
        Config.instance().getLogger(getClass().getName()).info("Searching for /items/search/" + q);
        List<Long> itemIds =  indexer.findIds(query,0);
        Config.instance().getLogger(getClass().getName()).info("SearchAdaptor: Query result size: " + itemIds.size());
        if (itemIds.isEmpty()){
            return;
        }

        Select sel = new Select().from(in.succinct.bpp.search.db.model.Item.class);
        Expression where = new Expression(sel.getPool(), Conjunction.AND);
        where.add(new Expression(sel.getPool(),"ACTIVE", Operator.EQ,true));


        if (!itemIds.isEmpty()){
            where.add(Expression.createExpression(sel.getPool(),"ID",Operator.IN,itemIds.toArray()));
        }

        List<in.succinct.bpp.search.db.model.Item> records = sel.where(where).execute(in.succinct.bpp.search.db.model.Item.class, 30);

        Set<Long> appIds = new HashSet<>();
        Set<Long> providerIds = new HashSet<>();
        Set<Long> providerLocationIds = new HashSet<>();
        Set<Long> fulfillmentIds = new HashSet<>();
        Set<Long> categoryIds = new HashSet<>();
        Set<Long> paymentIds = new HashSet<>();

        Cache<Long, Cache<Long, in.succinct.bpp.search.db.model.Item>> appItemMap = createAppDbCache(in.succinct.bpp.search.db.model.Item.class,new HashSet<>());
        records.forEach(i->{
            appIds.add(i.getApplicationId());
            providerIds.add(i.getProviderId());
            providerLocationIds.add(i.getProviderLocationId());
            fulfillmentIds.add(i.getFulfillmentId());
            categoryIds.add(i.getCategoryId());
            paymentIds.add(i.getPaymentId());
            appItemMap.get(i.getApplicationId()).put(i.getId(),i);
        });
        Cache<Long, Application> applicationCache = createDbCache(Application.class,appIds);
        Cache<Long, Cache<Long, in.succinct.bpp.search.db.model.Provider>> appProviderMap = createAppDbCache(in.succinct.bpp.search.db.model.Provider.class,providerIds);
        Cache<Long, Cache<Long, in.succinct.bpp.search.db.model.ProviderLocation>> appLocationMap = createAppDbCache(in.succinct.bpp.search.db.model.ProviderLocation.class,providerLocationIds);
        Cache<Long, Cache<Long, in.succinct.bpp.search.db.model.Fulfillment>> appFulfillmentMap = createAppDbCache(in.succinct.bpp.search.db.model.Fulfillment.class,fulfillmentIds);
        Cache<Long, Cache<Long, in.succinct.bpp.search.db.model.Category>> appCategoryMap = createAppDbCache(in.succinct.bpp.search.db.model.Category.class,categoryIds);
        Cache<Long, Cache<Long, in.succinct.bpp.search.db.model.Payment>> appPaymentMap = createAppDbCache(in.succinct.bpp.search.db.model.Payment.class,paymentIds);



        reply.setMessage(new Message());
        Catalog catalog = new Catalog();
        catalog.setDescriptor(new Descriptor());
        Subscriber subscriber = getSubscriber();
        catalog.getDescriptor().setName(adaptor.getProviderConfig().getStoreName());
        catalog.getDescriptor().setLongDesc(adaptor.getProviderConfig().getStoreName());
        catalog.getDescriptor().setShortDesc(adaptor.getProviderConfig().getStoreName());
        catalog.getDescriptor().setCode(subscriber.getSubscriberId());
        catalog.getDescriptor().setImages(new Images());
        catalog.getDescriptor().setSymbol(adaptor.getProviderConfig().getLogo());
        catalog.getDescriptor().getImages().add(adaptor.getProviderConfig().getLogo());

        reply.getMessage().setCatalog(catalog);
        Providers providers = new Providers();
        catalog.setProviders(providers);


        for (Long appId : appIds) {
            Application application = applicationCache.get(appId);

            Map<Long, in.succinct.bpp.search.db.model.Item> itemMap = appItemMap.get(appId);
            for (Long itemId : itemMap.keySet()) {
                Config.instance().getLogger(getClass().getName()).info("SearchAdaptor: Looping through result items" + itemId);
                in.succinct.bpp.search.db.model.Item dbItem = itemMap.get(itemId);

                in.succinct.bpp.search.db.model.Provider dbProvider = appProviderMap.get(appId).get(dbItem.getProviderId());
                ProviderLocation dbProviderLocation = appLocationMap.get(appId).get(dbItem.getProviderLocationId());
                Fulfillment dbFulfillment = appFulfillmentMap.get(appId).get(dbItem.getFulfillmentId());
                in.succinct.bpp.search.db.model.Category dbCategory = appCategoryMap.get(appId).get(dbItem.getCategoryId());
                Payment dbPayment = appPaymentMap.get(appId).get(dbItem.getPaymentId());

                Provider outProvider = providers.get(dbProvider.getObjectId());
                if (outProvider == null) {
                    outProvider = new Provider(dbProvider.getObjectJson());
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
                    providers.add(outProvider);

                }
                Categories categories = outProvider.getCategories();
                if (categories == null) {
                    categories = new Categories();
                    outProvider.setCategories(categories);
                }
                if (dbCategory != null) {
                    Category outCategory = categories.get(dbCategory.getObjectId());
                    if (outCategory == null) {
                        categories.add(new Category(dbCategory.getObjectJson()));
                    }
                }

                Locations locations = outProvider.getLocations();
                if (locations == null) {
                    locations = new Locations();
                    outProvider.setLocations(locations);
                }
                if (dbProviderLocation != null) {
                    Location outLocation = locations.get(dbProviderLocation.getObjectId());
                    if (outLocation == null) {
                        locations.add(new Location(dbProviderLocation.getObjectJson()));
                    }
                }

                Fulfillments fulfillments = outProvider.getFulfillments();
                if (fulfillments == null) {
                    fulfillments = new Fulfillments();
                    outProvider.setFulfillments(fulfillments);
                }
                if (dbFulfillment != null) {
                    in.succinct.beckn.Fulfillment outFulfillment = fulfillments.get(dbFulfillment.getObjectId());
                    if (outFulfillment == null) {
                        outFulfillment = new in.succinct.beckn.Fulfillment(dbFulfillment.getObjectJson());
                        fulfillments.add(outFulfillment);
                    }
                }

                Payments payments = outProvider.getPayments();
                if (payments == null) {
                    payments = new Payments();
                    outProvider.setPayments(payments);
                }
                in.succinct.beckn.Payment bapPaymentIntent = request.getMessage().getIntent().getPayment();
                if (dbPayment != null) {
                    if (payments.get(dbPayment.getObjectId()) == null) {
                        in.succinct.beckn.Payment payment = new in.succinct.beckn.Payment(dbPayment.getObjectJson());
                        if (bapPaymentIntent != null ) {
                            if (bapPaymentIntent.getBuyerAppFinderFeeType() == CommissionType.Percent){
                                if (bapPaymentIntent.getBuyerAppFinderFeeAmount() > adaptor.getProviderConfig().getMaxAllowedCommissionPercent()){
                                    throw new RuntimeException("Max commission percent exceeded");
                                }
                            }
                            payment.update(payment);
                        }
                        payments.add(payment);
                    }
                }
                Items items = outProvider.getItems();
                if (items == null) {
                    items = new Items();
                    outProvider.setItems(items);
                }
                if (items.get(dbItem.getObjectId()) == null) {
                    Item outItem = new Item(dbItem.getObjectJson());
                    if (outItem.getCategoryIds().size() > 0) {
                        outItem.setCategoryId(outItem.getCategoryIds().get(0));
                    }
                    if (outItem.getFulfillmentIds().size() > 0) {
                        outItem.setFulfillmentId(outItem.getFulfillmentIds().get(0));
                    }
                    if (outItem.getLocationIds().size() > 0) {
                        outItem.setLocationId(outItem.getLocationIds().get(0));
                    }
                    FulfillmentType outFulfillmentType = fulfillments.get(outItem.getFulfillmentId()).getType();
                    FulfillmentType inFulfillmentType = intentFulfillment == null ? null : intentFulfillment.getType();
                    FulfillmentStop end = intentFulfillment == null ? null : intentFulfillment.getEnd();

                    if (outFulfillmentType.matches(inFulfillmentType) ) {
                        Location storeLocation = locations.get(outItem.getLocationId());
                        if (end == null || adaptor.getProviderConfig().getServiceability(inFulfillmentType,end,storeLocation).isServiceable()){
                            outItem.setMatched(true);
                            outItem.setRelated(true);
                            outItem.setRecommended(true);

                            ItemQuantity itemQuantity = new ItemQuantity();
                            Quantity available =new Quantity() ;
                            available.setCount(adaptor.getProviderConfig().getMaxOrderQuantity()); // Ordering more than 20 is not allowed.
                            itemQuantity.setAvailable(available);
                            itemQuantity.setMaximum(available);

                            outItem.setItemQuantity(itemQuantity);
                            items.add(outItem);
                        }
                    }
                }

            }
        }


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

    private <T extends Model & IndexedApplicationModel> Cache<Long,Cache<Long,T>> createAppDbCache(Class<T> clazz, Set<Long> ids){
        Cache<Long,Cache<Long,T>> cache = new Cache<>(0,0) {
            @Override
            protected Cache<Long, T> getValue(Long applicationId) {
                return createDbCache(clazz,new HashSet<>());
            }
        };
        if (!ids.isEmpty() && !ids.contains(null)){
            Select select = new Select().from(clazz);
            select.where(new Expression(select.getPool(),"ID",Operator.IN,ids.toArray()));
            select.execute(clazz).forEach(t->cache.get(t.getApplicationId()).put(t.getId(),t));
        }
        return cache;
    }


    public void select( Request request, Request reply) {
        reply.setMessage(new Message());
        reply.getMessage().setOrder(getQuote(adaptor,request.getContext(),request.getMessage().getOrder()));
    }

    private Order getQuote(CommerceAdaptor adaptor, Context context, Order order) {
        adaptor.fixFulfillment(context,order);
        in.succinct.beckn.Fulfillment fulfillment = order.getFulfillment();
        FulfillmentStop end = fulfillment == null ? null : fulfillment.getEnd();

        adaptor.fixLocation(order);
        Location  providerLocation = order.getProviderLocation();

        Order finalOrder = new Order();
        finalOrder.setProvider(new Provider());
        finalOrder.getProvider().setId(adaptor.getSubscriber().getSubscriberId());
        finalOrder.setQuote(new Quote());
        finalOrder.setItems(new Items());
        finalOrder.getProvider().setLocations(new Locations());
        finalOrder.setFulfillments(new Fulfillments());

        Serviceability serviceability = null;
        if (fulfillment != null){
            finalOrder.getFulfillments().add(fulfillment);
            finalOrder.setFulfillment(fulfillment);
            if (end != null){
                serviceability = adaptor.getProviderConfig().getServiceability(fulfillment.getType(),end,providerLocation);
                if (serviceability.isServiceable()){
                    fulfillment.setState("serviceable");
                }
            }else {
                fulfillment.setState("serviceable");
            }
        }

        finalOrder.setProviderLocation(providerLocation);
        finalOrder.getProvider().getLocations().add(providerLocation);


        Price orderPrice = new Price();
        orderPrice.setCurrency("INR");
        BreakUp breakUp = new BreakUp();


        finalOrder.getQuote().setPrice(orderPrice);
        finalOrder.getQuote().setBreakUp(breakUp);


        BreakUpElement shipping_total = breakUp.createElement(BreakUpCategory.delivery,"Delivery Charges", new Price());
        shipping_total.getPrice().setCurrency(orderPrice.getCurrency());
        if (serviceability != null){
            shipping_total.getPrice().setValue(serviceability.getCharges());
            breakUp.add(shipping_total);
        }
        BreakUpElement tax_total = breakUp.createElement(BreakUpCategory.tax,"Tax", new Price());
        breakUp.add(tax_total);


        Items outItems = finalOrder.getItems();
        Items inItems = order.getItems();

        double deliveryTaxRate = 0 ;
        for (int i = 0 ; i < inItems.size() ; i ++ ){
            Item inItem = inItems.get(i);
            Quantity quantity = inItem.get(Quantity.class,"quantity");


            in.succinct.bpp.search.db.model.Item dbItem = getItem(adaptor,inItem.getId());
            if (dbItem == null ){
                throw new RuntimeException("No inventory with provider.");
            }
            Item outItem = new Item(dbItem.getObjectJson());
            outItem.setFulfillmentId(finalOrder.getFulfillment() == null ? null : finalOrder.getFulfillment().getId());



            double taxRate = dbItem.getReflector().getJdbcTypeHelper().getTypeRef(double.class).getTypeConverter().valueOf(outItem.getTags().get("tax_rate"));

            double configured_price  = outItem.getPrice().getValue(); //may be discounted price
            deliveryTaxRate = Math.max(deliveryTaxRate,taxRate);

            // price *(1+r/100) = configured_price
            // tax  = price * (r/100) = configured_price * (r/100)/(1+r/100)
            double current_price = adaptor.isTaxIncludedInPrice() ? configured_price / (1 + taxRate/100.0): configured_price;
            double regular_price = adaptor.isTaxIncludedInPrice() ? outItem.getPrice().getMaximumValue() / (1 + taxRate/100.0) : outItem.getPrice().getMaximumValue();

            Quantity avail = new Quantity(); avail.setCount(adaptor.getProviderConfig().getMaxOrderQuantity());

            ItemQuantity outQuantity = new ItemQuantity();
            outQuantity.setSelected(quantity);
            outQuantity.setAvailable(avail);
            outQuantity.setMaximum(avail);

            outItem.setItemQuantity(outQuantity);

            Price price = new Price();
            price.setCurrency("INR");
            price.setListedValue(regular_price * quantity.getCount());
            price.setOfferedValue(current_price * quantity.getCount());
            price.setValue(current_price * quantity.getCount());
            outItem.setPrice(price);


            Price tax = new Price();
            tax.setCurrency("INR");
            tax.setValue(taxRate/100.0 * price.getValue());
            tax.setListedValue(taxRate/100.0 * price.getListedValue());
            tax.setOfferedValue(taxRate/100.0 * price.getOfferedValue());
            outItem.setTax(tax);

            BreakUpElement element = breakUp.createElement(BreakUpCategory.item,outItem.getDescriptor().getName(),price);
            element.setItem(outItem);
            element.setItemId(outItem.getId());
            element.setItemQuantity(quantity);
            breakUp.add(element);


            orderPrice.setCurrency("INR");
            orderPrice.setListedValue(orderPrice.getListedValue() + price.getListedValue());
            orderPrice.setOfferedValue(orderPrice.getOfferedValue() + price.getOfferedValue());
            orderPrice.setValue(orderPrice.getValue() + price.getValue());

            Price orderTaxPrice = tax_total.getPrice();
            orderTaxPrice.setCurrency("INR");
            orderTaxPrice.setValue(orderTaxPrice.getValue() + tax.getValue());
            orderTaxPrice.setListedValue(orderTaxPrice.getListedValue() + tax.getListedValue());
            orderTaxPrice.setOfferedValue(orderTaxPrice.getOfferedValue() + tax.getOfferedValue());

            outItems.add(outItem);
        }

        Price orderTaxPrice = tax_total.getPrice();
        orderTaxPrice.setCurrency("INR");
        //Inlude shipping tax
        orderTaxPrice.setValue(orderTaxPrice.getValue() + deliveryTaxRate/100.0 * shipping_total.getPrice().getValue());
        orderTaxPrice.setListedValue(orderTaxPrice.getListedValue() + deliveryTaxRate/100.0 * shipping_total.getPrice().getListedValue());
        orderTaxPrice.setOfferedValue(orderTaxPrice.getOfferedValue() + deliveryTaxRate/100.0 * shipping_total.getPrice().getOfferedValue());


        orderPrice.setListedValue(orderPrice.getListedValue() + shipping_total.getPrice().getListedValue() + orderTaxPrice.getListedValue());
        orderPrice.setOfferedValue(orderPrice.getOfferedValue() +shipping_total.getPrice().getOfferedValue() +  orderTaxPrice.getOfferedValue());
        orderPrice.setValue(orderPrice.getValue() + shipping_total.getPrice().getValue() + orderTaxPrice.getValue());
        orderPrice.setCurrency("INR");




        finalOrder.getQuote().setTtl(15L*60L); //15 minutes.

        BecknOrderMeta meta = Database.getTable(BecknOrderMeta.class).newRecord();
        meta.setBecknTransactionId(context.getTransactionId());
        meta = Database.getTable(BecknOrderMeta.class).getRefreshed(meta);
        meta.setOrderJson(finalOrder.toString());
        meta.save();

        return finalOrder;
    }

    private in.succinct.bpp.search.db.model.Item getItem(CommerceAdaptor adaptor,String objectId) {

        Select select = new Select().from(in.succinct.bpp.search.db.model.Item.class);
        List<in.succinct.bpp.search.db.model.Item> dbItems = select.where(new Expression(select.getPool(), Conjunction.AND).
                add(new Expression(select.getPool(), "APPLICATION_ID", Operator.EQ, adaptor.getApplication().getId())).
                add(new Expression(select.getPool(), "OBJECT_ID", Operator.EQ, objectId))).execute(1);

        return dbItems.isEmpty() ? null : dbItems.get(0);
    }





}
