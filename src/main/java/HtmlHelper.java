import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import javafx.util.Pair;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;

import java.io.IOException;
import java.net.MalformedURLException;
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
public class HtmlHelper {
    private static final String PREFFIX = "http://list.mail.ru";

    public static void main(String[] args) throws IOException {
        final MongoDBStorage storage = MongoDBStorage.getInstance();
        List<String> test = new ArrayList<String>();
        test.add("http://list.mail.ru/18134/1/0_1_0_1.html");
        test.add("http://list.mail.ru/16185/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/27276/1/0_1_0_1.html");
        test.add("http://list.mail.ru/12403/1/0_1_0_1.html");
        test.add("http://list.mail.ru/33728/1/0_1_0_1.html");
        ExecutorService executorService = new BlockingThreadPoolExecutor(33,
                10, 1000L, TimeUnit.MILLISECONDS, 1000L, TimeUnit.MILLISECONDS, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return true;
            }
        });
        for (final String url : test) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Set<String> data =  getUrlsFromPages(url);
                        if (data.size()<=1){
                            storage.unparsedCollection.insert(BasicDBObjectBuilder.start().add(Field.URL, data).get());
                        }
                        System.out.println(new Date().toString()+"\t"+url+"\t"+data.size()+"");
                        for(String entity: data){
                            DBObject key =  BasicDBObjectBuilder.start().add(Field.URL,entity).get();
                           DBObject existing = storage.finalCollection.findOne(key);
                            if (existing!=null && existing.containsField(Field.rubric)){
                                existing.put(Field.rubric, existing.get(Field.rubric)+"|1");
                                storage.finalCollection.update(key,existing
                                        , true, false );
                            }  else {
                                storage.finalCollection.insert(BasicDBObjectBuilder.start().add(Field.URL, entity).add(Field.rubric,"1").get());
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });

        }

    }


    private static  Set<String> getUrlsFromPages(String startUrl) throws IOException {
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

            String nextUrl = PREFFIX + paging[paging.length - 1].getAttributeByName("href");
            if (paging[paging.length - 1].getAttributeByName("title").equals("следующая страница Ctrl &#8594;")) {
                processUrlsSinglePageAndMoveForward(nextUrl, urls);
            }
        } catch (Exception e) {
            System.out.println(currentPage + "\tERROR\t" + e.getMessage());
        }
    }

    private void getCatalogList() {
        MongoDBStorage storage = MongoDBStorage.getInstance();
        DBCursor url = storage.gottedCollection.find();
        HtmlCleaner cleaner = new HtmlCleaner();

        while (url.hasNext()) {
            Set<String> processed = new HashSet<String>();
            Long count = 0L;
            Queue<Pair<String, String>> subcategories = new LinkedList<Pair<String, String>>();
            Map<String, String> pages = new HashMap<String, String>();
            DBObject entity = url.next();

            subcategories.add(new Pair<String, String>((String) entity.get(Field.URL), (String) entity.get(Field.rubric)));
            while (!subcategories.isEmpty()) {
                try {
                    Pair<String, String> data = subcategories.remove();
                    count++;
                    TagNode root = cleaner.clean(new URL(data.getKey()), "windows-1251");

                    TagNode[] subc1 = root.getElementsByAttValue("name", "1", true, false);
                    TagNode[] subc0 = root.getElementsByAttValue("name", "0", true, false);

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
                            System.out.println("double");
                            pages.put(data.getKey(), data.getValue() + "|" + pages.get(data.getKey()));
                        } else {
                            pages.put(data.getKey(), data.getValue());
                        }

                    }
                    processed.add(data.getKey());
                    System.out.println(entity.get(Field.rubric).toString() + "\t" + count);
                } catch (Exception e) {
                    System.out.println(subcategories.peek());
                }
            }
            for (Map.Entry<String, String> catalogPage : pages.entrySet()) {
                storage.processedCollection.insert(BasicDBObjectBuilder.start()
                        .add(Field.URL, catalogPage.getKey())
                        .add(Field.rubric, catalogPage.getValue()).get());
            }
        }

    }

    private void getMain() throws IOException {
        HtmlCleaner cleaner = new HtmlCleaner();
        TagNode rootNode = cleaner.clean(new URL("http://list.mail.ru/"), "windows-1251");
        TagNode[] categories = rootNode.getElementsByName("h2", true);
        MongoDBStorage storage = MongoDBStorage.getInstance();
        String href;
        String cat;
        for (TagNode tagNode : categories) {
            href = tagNode.getElementsByName("a", false)[0].getAttributeByName("href");
            cat = tagNode.getText().toString();
            storage.gottedCollection.insert(BasicDBObjectBuilder.start()
                    .add(Field.rubric, cat)
                    .add(Field.URL, PREFFIX + href).get());
            System.out.println(cat);
        }
    }

}
