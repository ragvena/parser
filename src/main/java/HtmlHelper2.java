import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import javafx.util.Pair;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;

import java.io.IOException;
import java.net.URL;
import java.util.*;

/**
 * User: ragvena
 * Date: 4/2/13
 * Time: 10:03 PM
 */
public class HtmlHelper2 {
    private static final String PREFFIX = "http://list.mail.ru";

    public static void main(String[] args) throws IOException {


    }
    private void crawl(){
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
                            if (!processed.contains(PREFFIX + node.getAttributeByName("href").toString())){
                                subcategories.add(new Pair<String, String>(
                                        PREFFIX + node.getAttributeByName("href").toString(),
                                        data.getValue() + "/" + node.getText().toString()));
                            }
                        }
                        if (subc1 != null && subc1.length > 0) {
                            for (TagNode node : subc1) {
                                if (!processed.contains(PREFFIX + node.getAttributeByName("href").toString())){
                                    subcategories.add(new Pair<String, String>(
                                            PREFFIX + node.getAttributeByName("href").toString(),
                                            data.getValue() + "/" + node.getText().toString()));
                                }
                            }
                        }
                    } else {
                        if (pages.containsKey(data.getKey())){
                            System.out.println("double");
                            pages.put(data.getKey(), data.getValue()+"|"+pages.get(data.getKey()));
                        } else {
                            pages.put(data.getKey(), data.getValue());
                        }

                    }
                    processed.add(data.getKey());
                    System.out.println(entity.get(Field.rubric).toString()+"\t"+count);
                } catch (Exception e) {
                    System.out.println(subcategories.peek());
                }
            }
            for(Map.Entry<String,String> catalogPage: pages.entrySet()){
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

//
//    //Конструктор
//    public HtmlHelper(URL htmlPage) throws IOException
//    {
//        //Создаём объект HtmlCleaner
//        HtmlCleaner cleaner = new HtmlCleaner();
//        //Загружаем html код сайта
//
//        rootNode = cleaner.clean(htmlPage);
//    }
//
//    List<TagNode> getLinksByClass(String CSSClassname)
//    {
//        List<TagNode> linkList = new ArrayList<TagNode>();
//
//        //Выбираем все ссылки
//        TagNode linkElements[] = rootNode.getElementsByName("a", true);
//        for (int i = 0; linkElements != null && i < linkElements.length; i++)
//        {
//            //получаем атрибут по имени
//            String classType = linkElements[i].getAttributeByName("class");
//            //если атрибут есть и он эквивалентен искомому, то добавляем в список
//            if (classType != null && classType.equals(CSSClassname))
//            {
//                linkList.add(linkElements[i]);
//            }
//        }
//
//        return linkList;
//    }
}
