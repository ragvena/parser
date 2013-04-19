package ru.crystaldata.parser.mail;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import ru.crystaldata.parser.common.DataAnalyzer;
import ru.crystaldata.parser.common.RecordField;
import ru.crystaldata.parser.common.utils.MongoDBStorage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * User: ragvena
 * Date: 4/2/13
 * Time: 10:03 PM
 */
public class test {


    public static void main(String[] args) throws IOException {
        MongoDBStorage storage =  MongoDBStorage.getInstance();
        DBCursor data = storage.result.find();
        Map<String, String> domains = new HashMap<String, String>();
        while (data.hasNext()){
            DBObject record = data.next();
            String url = (String) record.get(RecordField.URL);
            String rubric = (String) record.get(RecordField.RUBRIC);
            String domain = DataAnalyzer.getUrlDomain(url);
            if (domains.containsKey(domain)){
                rubric+=":"+domains.get(domain);
            }
            domains.put(domain,rubric);

        }
        for(Map.Entry<String, String> entry: domains.entrySet()) {
            storage.classifiedDomain.insert(BasicDBObjectBuilder.start()
                    .add(RecordField.RUBRIC, entry.getValue())
                    .add(RecordField.DOMAIN, entry.getKey()).get());
        }
        int n=0;
    }


}
