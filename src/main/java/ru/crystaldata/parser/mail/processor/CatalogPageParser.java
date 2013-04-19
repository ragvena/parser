package ru.crystaldata.parser.mail.processor;

import com.mongodb.*;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;
import ru.crystaldata.parser.common.DataAnalyzer;
import ru.crystaldata.parser.common.RecordField;
import ru.crystaldata.parser.common.utils.BlockingThreadPoolExecutor;
import ru.crystaldata.parser.common.utils.MongoDBStorage;
import ru.crystaldata.parser.common.utils.NodeCondition;

import java.io.IOException;
import java.net.URL;
import java.util.*;
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

    public static void main(String[] args) throws IOException {

        final ConcurrentHashMap<String, ConcurrentHashMap<String, Double>> classifiedPage = new ConcurrentHashMap<String, ConcurrentHashMap<String, Double>>();
        ExecutorService executorService = new BlockingThreadPoolExecutor(100,
                100, 1000L, TimeUnit.MILLISECONDS, 1000L, TimeUnit.MILLISECONDS, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return true;
            }
        });
        Mongo mongo = new Mongo("localhost", 27017);
        mongo.setWriteConcern(WriteConcern.NORMAL);
        DB database = mongo.getDB("mailru");
        DBCollection mailru = database.getCollection("catalogpages");
        final DBCursor catalogPages = mailru.find();
        Integer threadNumber = catalogPages.count();
        final CountDownLatch latch = new CountDownLatch(threadNumber);
        final AtomicLong counter = new AtomicLong(0L);
        while (catalogPages.hasNext()) {
            DBObject page = catalogPages.next();
            final String startPage = (String) page.get(RecordField.URL);

            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    Map<String, Double> currentRubric = new HashMap<String, Double>();
                    try {
                        getPageRubrics(startPage, currentRubric);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    if (currentRubric.size() > 0) {
                    Set<String> domains = getDomainFromCatalogPage(startPage);

                        for (String domain : domains) {
                            if (classifiedPage.contains(domain)) {
                                mergeMaps(currentRubric, classifiedPage.get(domain));
                            }
                            classifiedPage.put(domain, new ConcurrentHashMap<String, Double>(currentRubric));
                        }
                    }
                    Long cThread = counter.incrementAndGet();
                    latch.countDown();
                }
            });

        }
        try {
            latch.await(2, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            storeClassifiedPages(classifiedPage.entrySet());
        }
        storeClassifiedPages(classifiedPage.entrySet());
    }

    public static void storeClassifiedPages(Set<Map.Entry<String, ConcurrentHashMap<String, Double>>> data) {
        for (Map.Entry<String, ConcurrentHashMap<String, Double>> url : data) {
            storage.result.insert(BasicDBObjectBuilder.start()
                    .add(RecordField.URL, url.getKey())
                    .add(RecordField.RUBRIC, url.getValue()).get());
        }
    }

    public static Set<String> getDomainFromCatalogPage(String catalogPageUrl) {
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
                    List<TagNode> urlList = root.getElementList(NodeCondition.MAILRU_URL_CONDITION, true);
                    if (urlList != null && urlList.size() > 0) {
                        pageCounter++;
                        for (TagNode singleUrl : urlList) {
                            urls.add(DataAnalyzer.getUrlDomain(singleUrl.getText().toString()));
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


    public static void getPageRubrics(String url, Map<String, Double> rubrics) throws IOException {
        HtmlCleaner cleaner = new HtmlCleaner();
        TagNode root = cleaner.clean(new URL(url), "windows-1251");
        TagNode[] breadCrumbs = root.getElementsByAttValue("class", "t95 b mb10", true, false);
        if (breadCrumbs.length == 1) {
//            очищаем результаты
            String[] subRubrics = breadCrumbs[0].getText().toString()
                    .replaceAll("\n", "")
                    .replaceAll("\\&raquo\\;", "/")
                    .replaceAll("[\\s]*/[\\s]*", "/")
                    .replaceAll("^[\\s]*", "")
                    .replaceAll("[\\s]*$", "")
                    .split("/");
//            от более общей к более детальной
            StringBuilder builder = new StringBuilder("");
            for (int i = 0; i < subRubrics.length; i++) {
                builder.append("/");
                builder.append(subRubrics[i].replaceAll("\\.",";"));
                if (rubrics.containsKey(builder.toString())) {
                    rubrics.put(builder.toString(), rubrics.get(builder.toString()) + 1);
                } else {
                    rubrics.put(builder.toString(), 1.0);
                }
            }
        } else {
            System.out.println(url);
        }
    }

    public static Map<String, Double> mergeMaps(Map<String, Double> firstAndResult, Map<String, Double> second) {
        for (Map.Entry<String, Double> subrubrics : second.entrySet()) {
            Double counter = subrubrics.getValue();
            if (firstAndResult.containsKey(subrubrics.getKey())) {
                counter += firstAndResult.get(subrubrics.getKey());
            }
            firstAndResult.put(subrubrics.getKey(), counter);
        }
        return firstAndResult;
    }


}
