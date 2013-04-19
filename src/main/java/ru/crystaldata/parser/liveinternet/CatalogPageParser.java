package ru.crystaldata.parser.liveinternet;

import com.mongodb.*;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;
import org.htmlcleaner.conditional.ITagNodeCondition;
import ru.crystaldata.parser.common.DataAnalyzer;
import ru.crystaldata.parser.common.RecordField;
import ru.crystaldata.parser.common.utils.BlockingThreadPoolExecutor;

import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;

/**
 * User: eyakovleva
 * Date: 4/18/13
 * Time: 4:05 PM
 */
public class CatalogPageParser {
    private static final String REQUEST_PARAMETER = "?page=";
    public static final ITagNodeCondition URL_CONDITION = new ITagNodeCondition() {
        @Override
        public boolean satisfy(TagNode tagNode) {
            if (tagNode.getName().equals("a")) {
                Map<String, String> attributes = tagNode.getAttributes();
                if (attributes.containsKey("href") &&
                        attributes.containsKey("name") &&
                        attributes.containsKey("target") &&
                        attributes.containsKey("onclick")) {
                    return true;
                }

            }
            return false;

        }
    };


    public static void main(String[] args) throws IOException {
        Mongo mongo = new Mongo("localhost");
        DB database = mongo.getDB("li");
        DBCollection liRubrics = database.getCollection("main");
        DBCollection rubricator = database.getCollection("rubricator");
        final DBCursor mainRubrics = liRubrics.find();


        ExecutorService executorService = new BlockingThreadPoolExecutor(1,
                5, 1000L, TimeUnit.MILLISECONDS, 1000L, TimeUnit.MILLISECONDS, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return true;
            }
        });
        Integer threadNumber = mainRubrics.count();
        final CountDownLatch latch = new CountDownLatch(threadNumber);
        final ConcurrentHashMap<String, ConcurrentSkipListSet<String>> classifiedPage = new ConcurrentHashMap<String, ConcurrentSkipListSet<String>>();
        while (mainRubrics.hasNext()) {

            DBObject currentRubric = mainRubrics.next();
            final String rubric = (String) currentRubric.get(RecordField.RUBRIC);
            final String startUrl = (String) currentRubric.get(RecordField.URL);
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    Set<String> domains = new HashSet<String>();
                    Integer pageIterator = 1;
                    String previousFirstDomain = "";
                    String currentFirstDomain = "epmty";
                    while (!previousFirstDomain.equals(currentFirstDomain)) {
                        List<String> domainsFromPage = null;
                        try {
                            domainsFromPage = getDoainsFromPage(startUrl + REQUEST_PARAMETER + pageIterator);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        previousFirstDomain = currentFirstDomain;
                        if (domainsFromPage != null && domainsFromPage.size() > 0) {
                            if (domainsFromPage.size() > 30) {
                                System.out.println(pageIterator);
                            }
                            currentFirstDomain = domainsFromPage.get(0);
                            domains.addAll(domainsFromPage);
                        }
                        pageIterator++;
                        if (pageIterator%100==0){
                            System.out.println(rubric+"\tprocessed\t"+pageIterator);
                        }
                    }
                    if (!domains.isEmpty()){
                        classifiedPage.put(rubric,new ConcurrentSkipListSet<String>(domains));
                    }
                    System.out.println(rubric+"\tfinish\t"+domains.size());
                    latch.countDown();

                }
            });
        }
        try {
            latch.await(4, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            storeClassifiedPages(classifiedPage.entrySet(), rubricator);
        }
        storeClassifiedPages(classifiedPage.entrySet(), rubricator);
    }

    public static void storeClassifiedPages(Set<Map.Entry<String, ConcurrentSkipListSet<String>>> data, DBCollection rubricator) {
        for (Map.Entry<String, ConcurrentSkipListSet<String>> rubric : data) {
            for(String domain: rubric.getValue()){
            rubricator.insert(BasicDBObjectBuilder.start()
                    .add(RecordField.DOMAIN,domain)
                    .add(RecordField.RUBRIC, rubric.getKey()).get());
            }
        }
    }


    public static List<String> getDoainsFromPage(String pageUrl) throws IOException {
        List<String> domains = new LinkedList<String>();
        HtmlCleaner cleaner = new HtmlCleaner();
        TagNode root = cleaner.clean(new URL(pageUrl), "utf-8");
        List<TagNode> rubricsNode = root.getElementList(URL_CONDITION, true);
        for (TagNode url : rubricsNode) {
            domains.add(DataAnalyzer.getUrlDomain(url.getAttributeByName("href").toString()));
        }
        return domains;

    }
}
