import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import javafx.util.Pair;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;
import org.htmlcleaner.conditional.ITagNodeCondition;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;

/**
 * User: ragvena
 * Date: 4/2/13
 * Time: 10:03 PM
 */
public class HtmlHelper {
    private static final String PREFFIX = "http://list.mail.ru";
    //    private static final String PREFFIX = "http:";
    static final ITagNodeCondition URL_CONDITION = new ITagNodeCondition() {
        @Override
        public boolean satisfy(TagNode tagNode) {
            if (tagNode.getName().equals("a")) {
                String href = tagNode.getAttributeByName("href");
                if (href != null && href.length() > 0) {
                    if (href.contains("site_jump.bat")) {
                        if (tagNode.getText().length() > 5 && tagNode.getText().subSequence(0, 4).equals("http"))
                            return true;
                    }
                }
            }
            return false;

        }
    };

    public static void main(String[] args) throws IOException {
        getCatalogList();
    }

    private static void parsePages() {
        final MongoDBStorage storage = MongoDBStorage.getInstance();
        ExecutorService executorService = new BlockingThreadPoolExecutor(33,
                10, 1000L, TimeUnit.MILLISECONDS, 1000L, TimeUnit.MILLISECONDS, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return true;
            }
        });
        final ConcurrentHashMap<String, Map<String, Double>> totalResult = new ConcurrentHashMap<String,  Map<String, Double>>();
        DBCursor pages = storage.processedCollection.find();
        final CountDownLatch latch = new CountDownLatch(((Number)storage.processedCollection.count()).intValue());
        while (pages.hasNext()) {
            DBObject page = pages.next();
            final String url = (String) page.get(Field.URL);
            final Map<String, Double> currentRubrics = (Map<String, Double>) page.get(Field.rubric);
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
//                        получить все урлы с учетом пейджинга
                        Set<String> urlFromCatalogPage = getUrlsFromPages(url);
                        System.out.println(new Date().toString() + "\t" + url + "\t" + urlFromCatalogPage.size() + "");
//ДЛЯ КАЖДОГО УРДА С СТРАНИЦЕ
                        for (String singleUrl : urlFromCatalogPage) {
//                            если такая страница уже была
                            Map<String, Double> addedRubrics = currentRubrics;
                            if (totalResult.contains(singleUrl)){
                                Map<String, Double> existingRubrics = totalResult.get(singleUrl);
                                for(Map.Entry<String, Double> singleExistingRubrics: existingRubrics.entrySet()){
                                    Double number = singleExistingRubrics.getValue();
                                    if (addedRubrics.containsKey(singleExistingRubrics.getKey())){

                                    }
                                }

                            }

                        }

                    } catch (IOException e) {
                        e.printStackTrace();
                        latch.countDown();
                    }
                    latch.countDown();
                }
            });

        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            // do nothing
        }

    }


    private static Set<String> getUrlsFromPages(String startUrl) throws IOException {
        Set<String> urls = new HashSet<String>();
        processUrlsSinglePageAndMoveForward(startUrl, urls);
        return urls;
    }

    private static void processUrlsSinglePageAndMoveForward(String currentPage, Set<String> urls) throws IOException {
        try {
            HtmlCleaner cleaner = new HtmlCleaner();
            TagNode rootNode = cleaner.clean(new URL(currentPage), "windows-1251");
            List<TagNode> urlList = rootNode.getElementList(URL_CONDITION, true);
            for (TagNode url : urlList) {
                urls.add(url.getText().toString());
            }
            TagNode[] paging = rootNode.getElementsByAttValue("id", "next_page", true, false);
            if (paging != null && paging.length != 0) {
                processUrlsSinglePageAndMoveForward(PREFFIX + paging[0].getText(), urls);
            }
        } catch (Exception e) {
            System.out.println(currentPage + "\tERROR\t" + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void getCatalogList() {
        final MongoDBStorage storage = MongoDBStorage.getInstance();
        DBCursor url = storage.gottedCollection.find();

        ExecutorService executorService = new BlockingThreadPoolExecutor(8,
                3, 1000L, TimeUnit.MILLISECONDS, 1000L, TimeUnit.MILLISECONDS, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return true;
            }
        });
        final CountDownLatch latch = new CountDownLatch(18);
        final ConcurrentHashMap<String, Map<String, Map<String, Double>>> totalResult = new ConcurrentHashMap<String, Map<String, Map<String, Double>>>();
        while (url.hasNext()) {
            final DBObject entity = url.next();


            final String mainRubric = (String) entity.get(Field.rubric);
            final String mainUrl = (String) entity.get(Field.URL);
            executorService.submit(new Runnable() {

                @Override
                public void run() {
                    Set<String> processedPages = new HashSet<String>();
                    Long count = 0L;
                    Queue<Pair<String, String>> catalogPagesQueue = new LinkedList<Pair<String, String>>();
                    Map<String, Map<String, Double>> catalogPages = new HashMap<String, Map<String, Double>>();

                    HtmlCleaner cleaner = new HtmlCleaner();

                    catalogPagesQueue.add(new Pair<String, String>(mainUrl, mainRubric));

                    while (!catalogPagesQueue.isEmpty()) {
                        try {
                            Pair<String, String> currentCatalogPage = catalogPagesQueue.remove();
                            count++;
                            TagNode root = cleaner.clean(new URL(currentCatalogPage.getKey()), "windows-1251");
                            //выявление подкатегорий
                            TagNode[] subc1 = root.getElementsByAttValue("name", "1", true, false);
                            TagNode[] subc0 = root.getElementsByAttValue("name", "0", true, false);
                            //проверка, есть ли на этой странице каталога url
                            List<TagNode> urlList = root.getElementList(URL_CONDITION, true);
                            //если на странице есть url,запоминаем ее
                            if (urlList != null && urlList.size() > 0) {
                                Map<String, Double> rubric = new HashMap<String, Double>();
                                if (catalogPages.containsKey(currentCatalogPage.getKey())) {
                                    rubric = catalogPages.get(currentCatalogPage.getKey());
                                }
                                parseRubrics(rubric, currentCatalogPage.getValue());
                                catalogPages.put(currentCatalogPage.getKey(), rubric);
                            }

                            //если на странице есть подкатегории, обрабатываем их
                            if (subc0 != null && subc0.length > 0) {

                                for (TagNode node : subc0) {
                                    if (!processedPages.contains(PREFFIX + node.getAttributeByName("href").toString())) {
                                        catalogPagesQueue.add(new Pair<String, String>(
                                                PREFFIX + node.getAttributeByName("href").toString(),
                                                currentCatalogPage.getValue() + "/" + node.getText().toString()));
                                    }
                                }
                                if (subc1 != null && subc1.length > 0) {
                                    for (TagNode node : subc1) {
                                        if (!processedPages.contains(PREFFIX + node.getAttributeByName("href").toString())) {
                                            catalogPagesQueue.add(new Pair<String, String>(
                                                    PREFFIX + node.getAttributeByName("href").toString(),
                                                    currentCatalogPage.getValue() + "/" + node.getText().toString()));
                                        }
                                    }
                                }
                            } else {
                                Map<String, Double> rubric = new HashMap<String, Double>();
                                if (catalogPages.containsKey(currentCatalogPage.getKey())) {
                                    rubric = catalogPages.get(currentCatalogPage.getKey());
                                }
                                parseRubrics(rubric, currentCatalogPage.getValue());
                                catalogPages.put(currentCatalogPage.getKey(), rubric);
                            }
                            processedPages.add(currentCatalogPage.getKey());
                            if (count % 500 == 0) {
                                System.out.println(new Date().toString() + "\t" + mainRubric + "\t" + count);
                            }
                        } catch (Exception e) {
                            System.out.println(new Date().toString() + "\t" + "Error" + catalogPagesQueue.peek());
                        }
                    }
                    System.out.println(new Date().toString() + "\t" + catalogPages.size() + "\t" + mainRubric);
                    totalResult.put(mainRubric, catalogPages);
                    try {
                        PrintWriter pw = new PrintWriter(new FileWriter(mainRubric));
                        for(Map.Entry<String, Map<String, Double>> entry:catalogPages.entrySet()){
                            pw.append(entry.getKey()+"\t"+entry.getValue()+"\n");
                        }
                        pw.close();

                    } catch (IOException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        latch.countDown();
                    }
                    System.out.println(new Date().toString() + "\t" + mainRubric + "\t processed");
                    latch.countDown();
                }
            });
        }
        try {
            latch.await(4, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            // do nothing
        }



        //для результатов каждого из потоков
        for (Map.Entry<String, Map<String, Map<String, Double>>> threadResult : totalResult.entrySet()) {
            //список каталоговых страниц для отдельной главной категории
            Map<String, Map<String, Double>> categoryUrls = threadResult.getValue();
//                каждая страница каталога
            for (Map.Entry<String, Map<String, Double>> currentPage : categoryUrls.entrySet()) {
                String currentPageUrl = currentPage.getKey();
                DBObject key = BasicDBObjectBuilder.start().add(Field.URL, currentPageUrl).get();
//                    добавляемый список рубрик
                Map<String, Double> currentRubric = currentPage.getValue();
                DBObject existingData = storage.processedCollection.findOne(key);
                if (existingData!=null && existingData.containsField(Field.rubric)){
//                        уже существующий в монге список рубрик
                    Map<String, Double> existingRubrics  = (Map<String, Double>) existingData.get(Field.rubric);
                    for(Map.Entry<String, Double> eRubric: existingRubrics.entrySet()){
                        Double counter = eRubric.getValue();
//                            если рубрика уже существует, складываем в количество вхождений
                        if (currentRubric.containsKey(eRubric.getKey())){
                            counter+=currentRubric.get(eRubric.getValue());
                        }
//                            в противном случае, добавляем в текущие рубрики существующую
                        currentRubric.put(eRubric.getKey(), counter);
                    }
                }
                storage.processedCollection.update(key, BasicDBObjectBuilder.start().add(Field.URL, currentPageUrl)
                        .add(Field.rubric, currentRubric).get(), true, false);
            }
        }

    }

    private static void getMain() throws IOException {
        HtmlCleaner cleaner = new HtmlCleaner();
        TagNode rootNode = cleaner.clean(new URL("http://list.mail.ru/"), "windows-1251");
        TagNode[] categories = rootNode.getElementsByName("h2", true);
        final MongoDBStorage storage = MongoDBStorage.getInstance();
        ExecutorService executorService = new BlockingThreadPoolExecutor(33,
                10, 1000L, TimeUnit.MILLISECONDS, 1000L, TimeUnit.MILLISECONDS, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return true;
            }
        });
        for (final TagNode tagNode : categories) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    String href = tagNode.getElementsByName("a", false)[0].getAttributeByName("href");
                    String cat = tagNode.getText().toString();
                    storage.gottedCollection.insert(BasicDBObjectBuilder.start()
                            .add(Field.rubric, cat)
                            .add(Field.URL, PREFFIX + href).get());
                    System.out.println(cat);
                }
            });
        }
    }

    private static void putRubric(Map<String, Double> data, String rubric) {
        if (data.containsKey(rubric)) {
            data.put(rubric, data.get(rubric) + 1.0);
        } else {
            data.put(rubric, 1.0);
        }
    }

    private static void parseRubrics(Map<String, Double> rubricMap, String currentRubric) {
        String cleanRubric = currentRubric
                .replaceAll("\\&nbsp;@", "")
                .replaceAll("\\&nbsp;", "");
        String[] subRubric = cleanRubric.split("/");
        StringBuilder rubricBuilder = new StringBuilder("");
        for (int i = 0; i < subRubric.length; i++) {
            rubricBuilder.append("/");
            rubricBuilder.append(subRubric[i]);
            putRubric(rubricMap, rubricBuilder.toString());
        }
    }

}
