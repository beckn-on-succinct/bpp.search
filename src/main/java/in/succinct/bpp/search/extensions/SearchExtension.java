package in.succinct.bpp.search.extensions;

import com.venky.extension.Registry;
import com.venky.swf.db.model.reflection.ModelReflector;
import com.venky.swf.plugins.lucene.index.LuceneIndexer;
import com.venky.swf.sql.Conjunction;
import com.venky.swf.sql.Expression;
import com.venky.swf.sql.Operator;
import com.venky.swf.sql.Select;
import in.succinct.beckn.Catalog;
import in.succinct.beckn.Category;
import in.succinct.beckn.Descriptor;
import in.succinct.beckn.Intent;
import in.succinct.beckn.Item;
import in.succinct.beckn.Message;
import in.succinct.beckn.Provider;
import in.succinct.beckn.Providers;
import in.succinct.beckn.Request;
import in.succinct.bpp.shell.extensions.BppActionExtension;
import org.apache.lucene.search.Query;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class SearchExtension extends BppActionExtension {
    static {
        Registry.instance().registerExtension("in.succinct.bpp.shell.extension",new SearchExtension());
    }
    @Override
    public void search(Request request, Request reply) {
        //request.getContext().
        Message message  = request.getMessage();
        Intent intent = message.getIntent();
        Descriptor intentDescriptor = intent == null ? null : intent.getDescriptor();
        StringBuilder q = new StringBuilder();
        if (intentDescriptor != null){
            q.append(intentDescriptor.getName());
        }else {
            Provider provider = intent == null ? null : intent.getProvider();
            Descriptor providerDescriptor = provider == null ? null : provider.getDescriptor();
            if (providerDescriptor != null){
                q.append(String.format("(PROVIDER_NAME:%s* OR PROVIDER_LOCATION_NAME:%s*)",providerDescriptor.getName(),providerDescriptor.getName()));
            }

            Item item = intent == null ? null : intent.getItem();
            Descriptor itemDescriptor = item == null ? null : item.getDescriptor();
            if (itemDescriptor != null){
                if (q.length() > 0){
                    q.append(" OR ");
                }
                q.append(String.format("ITEM_NAME:%s*",itemDescriptor.getName()));
            }

            Category category = intent == null ? null : intent.getCategory();
            Descriptor categoryDescriptor = category == null ? null : category.getDescriptor();
            if (categoryDescriptor != null){
                if (q.length() > 0){
                    q.append(" OR ");
                }
                q.append(String.format("ITEM_CATEGORY_NAME:%s*",categoryDescriptor.getName()));
            }
        }


        LuceneIndexer indexer = LuceneIndexer.instance(in.succinct.bpp.search.db.model.Provider.class);
        Query query = indexer.constructQuery(q.toString());
        List<Long> ids = indexer.findIds(query,0);

        Select sel = new Select().from(Item.class);
        sel.where(new Expression(sel.getPool(), Conjunction.AND)
                .add(Expression.createExpression(sel.getPool(), "ID", Operator.IN, ids.toArray()))
                .add(new Expression(sel.getPool(),"ACTIVE",Operator.EQ,true)))
                .orderBy(ModelReflector.instance(in.succinct.bpp.search.db.model.Provider.class).getOrderBy());
        List<in.succinct.bpp.search.db.model.Provider> records = sel.execute(in.succinct.bpp.search.db.model.Provider.class, 30);
        reply.setMessage(new Message());
        reply.getMessage().setCatalog(new Catalog());
        Providers providers = new Providers();
        reply.getMessage().getCatalog().setProviders(providers);

        records.forEach(record->{
            Provider provider = new Provider();
            provider.setInner((JSONObject) JSONValue.parse(record.getProviderJson()));
            if (providers.get(provider.getId()) == null){
                providers.add(provider);
            }else{
                merge(provider,providers.get(provider.getId()));
            }
        });
    }
    public void merge(Provider source,Provider target){
        //Source has only one item.
        Item item = source.getItems().get(0);

        target.getItems().add(item);
        if (target.getCategories().get(item.getCategoryId()) == null){
            target.getCategories().add(source.getCategories().get(item.getCategoryId()));
        }
        if (target.getLocations().get(item.getLocationId()) == null){
            target.getLocations().add(source.getLocations().get(item.getLocationId()));
        }

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
