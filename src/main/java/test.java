import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import javafx.util.Pair;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;

import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * User: ragvena
 * Date: 4/2/13
 * Time: 10:03 PM
 */
public class test {
    private static final String PREFFIX = "http://list.mail.ru";
//    private static final String PREFFIX = "http:";

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
        if (!rubricMap.containsKey(cleanRubric)) {
            String[] subRubric = cleanRubric.split("/");
            StringBuilder rubricBuilder = new StringBuilder("");
            for (int i = 0; i < subRubric.length; i++) {
                rubricBuilder.append("/");
                rubricBuilder.append(subRubric[i]);
                putRubric(rubricMap, rubricBuilder.toString());
            }
        }
    }

    public static void main(String[] args) throws IOException {
        String data = "Справки/Каталоги&nbsp;/Наука, Образование&nbsp;@/Рефераты&nbsp;@/Рефераты по медицине&nbsp;/Медицинские публикации&nbsp;@/Стоматология&nbsp;/Стоматологическая помощь&nbsp;@/Санкт-Петербург&nbsp;@|Справки/Каталоги&nbsp;/Наука, Образование&nbsp;@/Рефераты&nbsp;@/Рефераты по медицине&nbsp;/Медицинские публикации&nbsp;@/Стоматология&nbsp;/Стоматологическая помощь&nbsp;@/Санкт-Петербург&nbsp;@|Справки/Гурманам&nbsp;@/Трапеза&nbsp;@/Образ жизни&nbsp;@/Советы доктора&nbsp;@/Специализированные медицинские клиники&nbsp;@/Стоматологическая помощь&nbsp;/Санкт-Петербург&nbsp;@|Справки/Гурманам&nbsp;@/Трапеза&nbsp;@/Образ жизни&nbsp;@/Советы доктора&nbsp;@/Специализированные медицинские клиники&nbsp;@/Стоматологическая помощь&nbsp;/Санкт-Петербург&nbsp;@|Справки/Гурманам&nbsp;@/Трапеза&nbsp;@/Образ жизни&nbsp;@/Советы доктора&nbsp;@/Диагностика. Лечение. Реабилитация.&nbsp;@/Медицинское обслуживание в Санкт-Петербурге&nbsp;/Стоматологическая помощь&nbsp;|Справки/Гурманам&nbsp;@/Трапеза&nbsp;@/Образ жизни&nbsp;@/Советы доктора&nbsp;@/Диагностика. Лечение. Реабилитация.&nbsp;@/Медицинское обслуживание в Санкт-Петербурге&nbsp;/Стоматологическая помощь&nbsp;|Справки/Гурманам&nbsp;@/Трапеза&nbsp;@/Образ жизни&nbsp;@/Советы доктора&nbsp;@/к стоматологу&nbsp;/Стоматологическая помощь&nbsp;@/Санкт-Петербург&nbsp;@|Справки/Гурманам&nbsp;@/Трапеза&nbsp;@/Образ жизни&nbsp;@/Советы доктора&nbsp;@/к стоматологу&nbsp;/Стоматологическая помощь&nbsp;@/Санкт-Петербург&nbsp;@|Компьютеры/Программирование&nbsp;/Учеба в ВУЗе&nbsp;@/Рефераты&nbsp;@/Рефераты по медицине&nbsp;/Медицинские публикации&nbsp;@/Стоматология&nbsp;/Стоматологическая помощь&nbsp;@/Санкт-Петербург&nbsp;@|Компьютеры/Программирование&nbsp;/Учеба в ВУЗе&nbsp;@/Рефераты&nbsp;@/Рефераты по медицине&nbsp;/Медицинские публикации&nbsp;@/Стоматология&nbsp;/Стоматологическая помощь&nbsp;@/Санкт-Петербург&nbsp;@";
        Map<String, Double> r = new HashMap<String, Double>();

        String [] test = data.split("\\|");
        for(String datatest: test){
            parseRubrics(r, datatest);
        }

        final Comparator<Map.Entry<String, Double>> COMPARATOR = new Comparator<Map.Entry<String, Double>>() {
            public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        };
        List<Map.Entry<String, Double>> lis = new ArrayList<Map.Entry<String, Double>>(r.entrySet());
        Collections.sort(lis, COMPARATOR);
        int n = 0;


//
//
//        ITagNodeCondition urlConditions = new ITagNodeCondition() {
//            @Override
//            public boolean satisfy(TagNode tagNode) {
//                if (tagNode.getName().equals("a")) {
//                    String href = tagNode.getAttributeByName("href");
//                    if (href != null && href.length() > 0) {
//                        if (href.contains("site_jump.bat")) {
//                            if (tagNode.getText().length()>5 && tagNode.getText().subSequence(0, 4).equals("http"))
//                            return true;
//                        }
//                    }
//                }
//                return false;
//
//            }
//        };
//        ITagNodeCondition nextPageCondition = new ITagNodeCondition() {
//            @Override
//            public boolean satisfy(TagNode tagNode) {
//               if (tagNode.getName().equals("a")){
//                   if (tagNode.hasAttribute("title")){
//                       if (tagNode.getAttributeByName("title").contains("следующая страница")){
//                           return  true;
//                       }
//                   }
//               }
//                return false;
//
//            }
//        };
//        Set<String> processed = new HashSet<String>();
//        Long count = 0L;
//        Queue<Pair<String, String>> subcategories = new LinkedList<Pair<String, String>>();
//        Map<String, String> pages = new HashMap<String, String>();
//
//        HtmlCleaner cleaner = new HtmlCleaner();
//        subcategories.add(new Pair<String, String>("http://list.mail.ru/11109/1/0_1_0_1.html", "1"));
//        while (!subcategories.isEmpty()) {
//            try {
//                Pair<String, String> data = subcategories.remove();
//                count++;
//                TagNode root = cleaner.clean(new URL(data.getKey()), "windows-1251");
//                //выявление подкатегорий
//                TagNode[] subc1 = root.getElementsByAttValue("name", "1", true, false);
//                TagNode[] subc0 = root.getElementsByAttValue("name", "0", true, false);
//                //проверка, есть ли на этой странице каталога url
//                List<TagNode> urlsOnCurrentPage = root.getElementList(nextPageCondition, true);
//                TagNode[] test = root.getElementsByAttValue("id","next_page", true, false);
//                //если на странице есть url,запоминаем ее
//                if (urlsOnCurrentPage != null) {
//                    if (pages.containsKey(data.getKey())) {
//                        pages.put(data.getKey(), data.getValue() + "|" + pages.get(data.getKey()));
//                    } else {
//                        pages.put(data.getKey(), data.getValue());
//                    }
//
//                }
//                //если на странице есть подкатегории, обрабатываем их
//                if (subc0 != null && subc0.length > 0) {
//
//                    for (TagNode node : subc0) {
//                        if (!processed.contains(PREFFIX + node.getAttributeByName("href").toString())) {
//                            subcategories.add(new Pair<String, String>(
//                                    PREFFIX + node.getAttributeByName("href").toString(),
//                                    data.getValue() + "/" + node.getText().toString()));
//                        }
//                    }
//                    if (subc1 != null && subc1.length > 0) {
//                        for (TagNode node : subc1) {
//                            if (!processed.contains(PREFFIX + node.getAttributeByName("href").toString())) {
//                                subcategories.add(new Pair<String, String>(
//                                        PREFFIX + node.getAttributeByName("href").toString(),
//                                        data.getValue() + "/" + node.getText().toString()));
//                            }
//                        }
//                    }
//                } else {
//                    if (pages.containsKey(data.getKey())) {
//                        pages.put(data.getKey(), data.getValue() + "|" + pages.get(data.getKey()));
//                    } else {
//                        pages.put(data.getKey(), data.getValue());
//                    }
//
//                }
//                processed.add(data.getKey());
//                if (count % 1000 == 0) {
//                    System.out.println(new Date().toString() + "\t" + "1" + "\t" + count);
//                }
//            } catch (Exception e) {
//                System.out.println(new Date().toString() + "\t" + "Error" + subcategories.peek());
//            }
//        }
    }

    private static void parsePages() {
        final MongoDBStorage storage = MongoDBStorage.getInstance();
        List<String> test = new ArrayList<String>();
        ExecutorService executorService = new BlockingThreadPoolExecutor(33,
                10, 1000L, TimeUnit.MILLISECONDS, 1000L, TimeUnit.MILLISECONDS, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return true;
            }
        });
        DBCursor pages = storage.processedCollection.find();
        while (pages.hasNext()) {
            final DBObject page = pages.next();
            final String url = (String) page.get(Field.URL);
            final String rubrics = (String) page.get(Field.rubric);
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Set<String> data = getUrlsFromPages(url);
                        if (data.size() <= 1) {
                            storage.unparsedCollection.insert(BasicDBObjectBuilder.start().add(Field.URL, data).get());
                        }
                        System.out.println(new Date().toString() + "\t" + url + "\t" + data.size() + "");

                        for (String entity : data) {
                            DBObject key = BasicDBObjectBuilder.start().add(Field.URL, entity).get();
                            DBObject existing = storage.finalCollection.findOne(key);
                            if (existing != null && existing.containsField(Field.rubric)) {
                                existing.put(Field.rubric, existing.get(Field.rubric) + "|" + rubrics);
                                storage.finalCollection.update(key, existing, true, false);
                            } else {
                                storage.finalCollection.insert(BasicDBObjectBuilder.start()
                                        .add(Field.URL, entity)
                                        .add(Field.rubric, rubrics).get());
                            }
                        }

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });

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
            TagNode[] categories = rootNode.getElementsByAttValue("class", "rez-descr", true, false);
            for (int i = 0; i < categories.length; i += 2) {
                urls.add(categories[i].getText().toString().split("\n")[1]);
            }
            TagNode[] paging = rootNode.getElementsByAttValue("class", "mb10 mt10 t100", true, false)[0].getElementsHavingAttribute("title", false);
            if (paging != null && paging.length != 0) {
                String nextUrl = PREFFIX + paging[paging.length - 1].getAttributeByName("href");
                if (paging[paging.length - 1].getAttributeByName("title").equals("следующая страница Ctrl &#8594;")) {
                    processUrlsSinglePageAndMoveForward(nextUrl, urls);
                }
            }
        } catch (Exception e) {
            System.out.println(currentPage + "\tERROR\t" + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void getCatalogList() {
        final MongoDBStorage storage = MongoDBStorage.getInstance();
        DBCursor url = storage.gottedCollection.find();

        ExecutorService executorService = new BlockingThreadPoolExecutor(18,
                3, 1000L, TimeUnit.MILLISECONDS, 1000L, TimeUnit.MILLISECONDS, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return true;
            }
        });
        while (url.hasNext()) {
            final DBObject entity = url.next();
            executorService.submit(new Runnable() {

                @Override
                public void run() {
                    Set<String> processed = new HashSet<String>();
                    Long count = 0L;
                    Queue<Pair<String, String>> subcategories = new LinkedList<Pair<String, String>>();
                    Map<String, String> pages = new HashMap<String, String>();

                    HtmlCleaner cleaner = new HtmlCleaner();
                    subcategories.add(new Pair<String, String>((String) entity.get(Field.URL), (String) entity.get(Field.rubric)));
                    while (!subcategories.isEmpty()) {
                        try {
                            Pair<String, String> data = subcategories.remove();
                            count++;
                            TagNode root = cleaner.clean(new URL(data.getKey()), "windows-1251");
                            //выявление подкатегорий
                            TagNode[] subc1 = root.getElementsByAttValue("name", "1", true, false);
                            TagNode[] subc0 = root.getElementsByAttValue("name", "0", true, false);
                            //проверка, есть ли на этой странице каталога url
                            TagNode[] urlsOnCurrentPage = root.getElementsByAttValue("class", "rez-descr", true, false);
                            //если на странице есть url,запоминаем ее
                            if (urlsOnCurrentPage != null && urlsOnCurrentPage.length % 2 == 0) {
                                if (pages.containsKey(data.getKey())) {
                                    pages.put(data.getKey(), data.getValue() + "|" + pages.get(data.getKey()));
                                } else {
                                    pages.put(data.getKey(), data.getValue());
                                }

                            }
                            //если на странице есть подкатегории, обрабатываем их
                            if (subc0 != null && subc0.length > 0) {

                                for (TagNode node : subc0) {
                                    if (!processed.contains(PREFFIX + node.getAttributeByName("href").toString())) {
                                        subcategories.add(new Pair<String, String>(
                                                PREFFIX + node.getAttributeByName("href").toString(),
                                                data.getValue() + "/" + node.getText().toString()));
                                    }
                                }
                                if (subc1 != null && subc1.length > 0) {
                                    for (TagNode node : subc1) {
                                        if (!processed.contains(PREFFIX + node.getAttributeByName("href").toString())) {
                                            subcategories.add(new Pair<String, String>(
                                                    PREFFIX + node.getAttributeByName("href").toString(),
                                                    data.getValue() + "/" + node.getText().toString()));
                                        }
                                    }
                                }
                            } else {
                                if (pages.containsKey(data.getKey())) {
                                    pages.put(data.getKey(), data.getValue() + "|" + pages.get(data.getKey()));
                                } else {
                                    pages.put(data.getKey(), data.getValue());
                                }

                            }
                            processed.add(data.getKey());
                            if (count % 1000 == 0) {
                                System.out.println(new Date().toString() + "\t" + entity.get(Field.rubric).toString() + "\t" + count);
                            }
                        } catch (Exception e) {
                            System.out.println(new Date().toString() + "\t" + "Error" + subcategories.peek());
                        }
                    }
                    System.out.println(new Date().toString() + "\t" + pages.size() + "\t" + entity.get(Field.rubric));

                    for (Map.Entry<String, String> catalogPage : pages.entrySet()) {
                        DBObject key = new BasicDBObject(Field.URL, catalogPage.getKey());
                        //если такая страница уже есть, добавить к ней текущую рубрику
                        DBObject existing = storage.processedCollection.findOne(key);
                        if (existing != null && existing.containsField(Field.rubric)) {
                            existing.put(Field.rubric, existing.get(Field.rubric) + "|" + catalogPage.getValue());
                            storage.processedCollection.update(key, existing, true, false);
                            System.out.println("double");
                        } else {
                            storage.processedCollection.insert(BasicDBObjectBuilder.start()
                                    .add(Field.URL, catalogPage.getKey())
                                    .add(Field.rubric, catalogPage.getValue()).get());
                        }
                    }
                }
            });

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

}
