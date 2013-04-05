package ru.mail.parser;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;
import ru.mail.RecordField;
import ru.mail.utils.BlockingThreadPoolExecutor;
import ru.mail.utils.MongoDBStorage;
import ru.mail.utils.NodeCondition;

import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: eyakovleva
 * Date: 4/5/13
 * Time: 4:33 PM
 */
public class CatalogPageParser {
    private static final MongoDBStorage storage = MongoDBStorage.getInstance();
    public static void main(String[] args) {

        final ConcurrentHashMap<String, String> classifiedPage = new ConcurrentHashMap<String, String>();
        ExecutorService executorService = new BlockingThreadPoolExecutor(20,
                10, 1000L, TimeUnit.MILLISECONDS, 1000L, TimeUnit.MILLISECONDS, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return true;
            }
        });
        final DBCursor catalogPages = storage.result.find();
        Integer threadNumber = catalogPages.count();
        final CountDownLatch latch = new CountDownLatch(threadNumber);
        final AtomicLong counter = new AtomicLong(0L);
        while (catalogPages.hasNext()) {
            DBObject page = catalogPages.next();
            final String startPage = (String) page.get(RecordField.URL);
            final String rubric = (String) page.get(RecordField.RUBRIC);
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    Set<String> urls = getUrlsFromCatalogPage(startPage);
                    if (urls.size()>0){
                        for(String url: urls){
                            String addedRubric = rubric;
                            if (classifiedPage.contains(url)){
                                addedRubric+=":"+classifiedPage.get(url);
                            }
                            classifiedPage.put(url, addedRubric);
                        }
                    }
                    Long cThread =  counter.incrementAndGet();
                    System.out.println(cThread+"\t"+urls.size());
                    latch.countDown();
                }
            });

        }
        try {
            latch.await(4, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            storeClassifiedPages(classifiedPage);
        }
        storeClassifiedPages(classifiedPage);
    }
    public static void  storeClassifiedPages(Map<String,String> data){
        for(Map.Entry<String, String> url:data.entrySet()){
             storage.result.insert(BasicDBObjectBuilder.start()
                     .add(RecordField.URL, url.getKey())
                     .add(RecordField.RUBRIC, url.getValue()).get());
        }
    }

    public static Set<String> getUrlsFromCatalogPage(String catalogPageUrl) {
        Set<String> urls = new HashSet<String>();
        Pattern paginator = Pattern.compile("(.*/0_1_0_)[\\d]+.html");
        Matcher matcher = paginator.matcher(catalogPageUrl);
        if (matcher.find()) {
            Boolean thereIsNextPage = true;
            Integer pageCounter = 1;
            final String postfix = ".html";
            final String prefix = matcher.group(1);
            HtmlCleaner cleaner = new HtmlCleaner();
            while (thereIsNextPage) {
                try {
                    TagNode root = cleaner.clean(new URL(prefix + pageCounter + postfix), "windows-1251");
                    List<TagNode> urlList = root.getElementList(NodeCondition.URL_CONDITION, true);
                    if (urlList != null && urlList.size() > 0) {
                        pageCounter++;
                        for (TagNode singleUrl : urlList) {
                            urls.add(singleUrl.getText().toString());
                        }
                    } else {
                        thereIsNextPage = false;
                    }
                } catch (Exception e) {
                    thereIsNextPage = false;
                }
            }
        }
        return urls;
    }


}
