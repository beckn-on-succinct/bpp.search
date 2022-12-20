package in.succinct.bpp.search.extensions;

import com.venky.cache.Cache;
import com.venky.extension.Registry;
import com.venky.swf.db.Database;
import com.venky.swf.db.model.Model;
import com.venky.swf.db.model.application.Application;
import com.venky.swf.plugins.lucene.index.LuceneIndexer;
import com.venky.swf.sql.Conjunction;
import com.venky.swf.sql.Expression;
import com.venky.swf.sql.Operator;
import com.venky.swf.sql.Select;
import in.succinct.beckn.Catalog;
import in.succinct.beckn.Categories;
import in.succinct.beckn.Category;
import in.succinct.beckn.Descriptor;
import in.succinct.beckn.Fulfillments;
import in.succinct.beckn.Intent;
import in.succinct.beckn.Item;
import in.succinct.beckn.Items;
import in.succinct.beckn.Location;
import in.succinct.beckn.Locations;
import in.succinct.beckn.Message;
import in.succinct.beckn.Payments;
import in.succinct.beckn.Provider;
import in.succinct.beckn.Providers;
import in.succinct.beckn.Request;
import in.succinct.beckn.Tag;
import in.succinct.beckn.TagGroup;
import in.succinct.beckn.Tags;
import in.succinct.bpp.search.db.model.Fulfillment;
import in.succinct.bpp.search.db.model.IndexedApplicationModel;
import in.succinct.bpp.search.db.model.Payment;
import in.succinct.bpp.search.db.model.ProviderLocation;
import in.succinct.bpp.shell.extensions.BppActionExtension;
import org.apache.lucene.search.Query;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SearchExtension extends BppActionExtension {
    static {
        Registry.instance().registerExtension("in.succinct.bpp.shell.extension",new SearchExtension());
    }


    @Override
    public void search(Request request, Request reply) {
        try{
            _search(request,reply);
        }catch (Exception ex){

        }
    }
    public void _search(Request request, Request reply) {
      //request.getContext().
        Message message  = request.getMessage();
        Intent intent = message.getIntent();
        Descriptor intentDescriptor = intent == null ? null : intent.getDescriptor();

        Provider provider = intent == null ? null : intent.getProvider();
        Descriptor providerDescriptor = provider == null ? intentDescriptor : provider.getDescriptor();


        Item item = intent == null ? null : intent.getItem();
        Descriptor itemDescriptor = item == null ? intentDescriptor : item.getDescriptor();

        Category category = intent == null ? null : intent.getCategory();
        Descriptor categoryDescriptor = category == null ? intentDescriptor : category.getDescriptor();

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
        List<Long> itemIds =  indexer.findIds(query,0);
        if (itemIds.isEmpty()){
            return;
        }

        Select sel = new Select().from(in.succinct.bpp.search.db.model.Item.class);
        Expression where = new Expression(sel.getPool(), Conjunction.AND);
        where.add(new Expression(sel.getPool(),"ACTIVE",Operator.EQ,true));


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
        reply.getMessage().setCatalog(catalog);
        Providers providers = new Providers();
        catalog.setProviders(providers);


        for (Long appId : appIds) {
            Application application = applicationCache.get(appId);

            Map<Long, in.succinct.bpp.search.db.model.Item> itemMap = appItemMap.get(appId);
            for (Long itemId : itemMap.keySet()) {
                in.succinct.bpp.search.db.model.Item dbItem = itemMap.get(itemId);

                in.succinct.bpp.search.db.model.Provider dbProvider = appProviderMap.get(appId).get(dbItem.getProviderId());
                ProviderLocation dbProviderLocation = appLocationMap.get(appId).get(dbItem.getProviderLocationId());
                Fulfillment dbFulfillment = appFulfillmentMap.get(appId).get(dbItem.getFulfillmentId());
                in.succinct.bpp.search.db.model.Category dbCategory = appCategoryMap.get(appId).get(dbItem.getCategoryId());
                Payment dbPayment = appPaymentMap.get(appId).get(dbItem.getPaymentId());

                Provider outProvider = providers.get(dbProvider.getObjectId());
                if (outProvider == null) {
                    outProvider = new Provider(dbProvider.getObjectJson());
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
                    providers.add(outProvider);
                }
                Categories categories = outProvider.getCategories();
                if (categories == null) {
                    categories = new Categories();
                    outProvider.setCategories(categories);
                }
                Category outCategory = categories.get(dbCategory.getObjectId());
                if (outCategory == null) {
                    outCategory = new Category(dbCategory.getObjectJson());
                    categories.add(outCategory);
                }
                Locations locations = outProvider.getLocations();
                if (locations == null) {
                    locations = new Locations();
                    outProvider.setLocations(locations);
                }
                Location outLocation = locations.get(dbProviderLocation.getObjectId());
                if (outLocation == null) {
                    outLocation = new Location(dbProviderLocation.getObjectJson());
                    locations.add(outLocation);
                }

                Fulfillments fulfillments = outProvider.getFulfillments();
                if (fulfillments == null) {
                    fulfillments = new Fulfillments();
                    outProvider.setFulfillments(fulfillments);
                }
                in.succinct.beckn.Fulfillment outFulfillment = fulfillments.get(dbFulfillment.getObjectId());
                if (outFulfillment == null) {
                    outFulfillment = new in.succinct.beckn.Fulfillment(dbFulfillment.getObjectJson());
                    fulfillments.add(outFulfillment);
                }

                Payments payments = outProvider.getPayments();
                if (payments == null) {
                    payments = new Payments();
                    outProvider.setPayments(payments);
                }
                if (payments.get(dbPayment.getObjectId()) == null) {
                    payments.add(new in.succinct.beckn.Payment(dbPayment.getObjectJson()));
                }
                Items items = outProvider.getItems();
                if (items == null) {
                    items = new Items();
                    outProvider.setItems(items);
                }
                if (items.get(dbItem.getObjectId()) == null) {
                    items.add(new Item(dbItem.getObjectJson()));
                }

            }
        }


    }
    private <T extends Model> Cache<Long,T> createDbCache(Class<T> clazz, Set<Long> ids) {
        Cache<Long,T> cache = new Cache<Long,T>(0,0){

            @Override
            protected T getValue(Long id) {
                return Database.getTable(clazz).get(id);
            }
        };
        if (!ids.isEmpty()){
            Select select = new Select().from(clazz);
            select.where(new Expression(select.getPool(),"ID",Operator.IN,ids.toArray()));
            select.execute(clazz).forEach(t->cache.put(t.getId(),t));
        }
        return cache;
    }

    private <T extends Model & IndexedApplicationModel> Cache<Long,Cache<Long,T>> createAppDbCache(Class<T> clazz, Set<Long> ids){
        Cache<Long,Cache<Long,T>> cache = new Cache<Long, Cache<Long, T>>(0,0) {
            @Override
            protected Cache<Long, T> getValue(Long applicationId) {
                return createDbCache(clazz,new HashSet<>());
            }
        };
        if (!ids.isEmpty()){
            Select select = new Select().from(clazz);
            select.where(new Expression(select.getPool(),"ID",Operator.IN,ids.toArray()));
            select.execute(clazz).forEach(t->cache.get(t.getApplicationId()).put(t.getId(),t));
        }
        return cache;
    }

    @Override
    public void select(Request request, Request response) {
        
    }

    @Override
    public void init(Request request, Request reply) {
        
    }

    @Override
    public void confirm(Request request, Request reply) {
        
    }

    @Override
    public void track(Request request, Request reply) {
        
    }

    @Override
    public void cancel(Request request, Request reply) {
        
    }

    @Override
    public void update(Request request, Request reply) {
        
    }

    @Override
    public void status(Request request, Request reply) {
        
    }

    @Override
    public void rating(Request request, Request reply) {
        
    }


    @Override
    public void support(Request request, Request reply) {
        
    }

    @Override
    public void get_cancellation_reasons(Request request, Request reply) {
        
    }

    @Override
    public void get_return_reasons(Request request, Request reply) {
        
    }

    @Override
    public void get_rating_categories(Request request, Request reply) {
        
    }

    @Override
    public void get_feedback_categories(Request request, Request reply) {
        
    }
}
