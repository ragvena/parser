package ru.crystaldata.parser.liveinternet;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;
import org.htmlcleaner.conditional.ITagNodeCondition;
import ru.crystaldata.parser.common.RecordField;

import java.io.IOException;
import java.net.URL;
import java.util.List;

/**
 * User: eyakovleva
 * Date: 4/18/13
 * Time: 3:18 PM
 */
public class MainPageParser {
    private static String PREFFIX = "http://www.liveinternet.ru";
    public static final ITagNodeCondition RUBRIC_CONDITION = new ITagNodeCondition() {
        @Override
        public boolean satisfy(TagNode tagNode) {
            if (tagNode.getName().equals("a")) {
                String href = tagNode.getAttributeByName("href");
                if (href != null && href.length() > 0) {
                    if (href.contains("/rating/")) {
                        String[] elements = href.split("/");
                        if (elements.length >= 3 && elements[2].length() > 2)
                            return true;
                    }
                }
            }
            return false;

        }
    };


    public static void main(String[] args) throws IOException {
        Mongo mongo = new Mongo("localhost");
        DB database = mongo.getDB("li");
        DBCollection liRubrics = database.getCollection("main");
        HtmlCleaner cleaner = new HtmlCleaner();
        TagNode root = cleaner.clean(new URL("http://www.liveinternet.ru/rating/month.html"), "utf-8");
        List<TagNode> rubricsNode = root.getElementList(RUBRIC_CONDITION, true);
        for (TagNode tag : rubricsNode) {
            liRubrics.insert(BasicDBObjectBuilder.start()
                    .add(RecordField.RUBRIC, tag.getText().toString())
                    .add(RecordField.URL, PREFFIX+tag.getAttributes().get("href").toString())
                    .get());
            System.out.println(tag.getAttributes().get("href"));
        }
    }
}
