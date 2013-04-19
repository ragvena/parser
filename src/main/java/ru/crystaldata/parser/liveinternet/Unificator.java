package ru.crystaldata.parser.liveinternet;

import com.mongodb.*;
import ru.crystaldata.parser.common.Domain;
import ru.crystaldata.parser.common.RecordField;

import java.io.IOException;
import java.util.*;

/**
 * User: eyakovleva
 * Date: 4/19/13
 * Time: 8:36 AM
 */
public class Unificator {
    public static void main(String[] args) throws IOException {
        Mongo mongo = new Mongo("localhost");
        DB database = mongo.getDB("li");
        DBCollection rubricator = database.getCollection("rubricator");

        DB rubricatorDatabase = mongo.getDB("rubricator");
        DBCollection liveinternet = rubricatorDatabase.getCollection("liveinternet");
        DBCursor rubricatorCursor = rubricator.find();
        Map<String,Domain> result = new HashMap<String, Domain>();
        while (rubricatorCursor.hasNext()){
            DBObject current = rubricatorCursor.next();
            String domain = (String) current.get(RecordField.DOMAIN);
            String rubric = "/"+(String) current.get(RecordField.RUBRIC);
            Set<String> source = new HashSet<String>(Arrays.asList("liveinternet"));
            Domain currentDomainInfo = new Domain(source, rubric);
            if (result.containsKey(domain)){
               currentDomainInfo.merge(result.get(domain));
            }
            result.put(domain, currentDomainInfo);
        }

        for(Map.Entry<String, Domain> entity: result.entrySet()) {
            liveinternet.insert(BasicDBObjectBuilder.start()
                    .add(RecordField.DOMAIN, entity.getKey())
                    .add(RecordField.RUBRIC, entity.getValue().rubric)
                    .add(RecordField.SOURCE, entity.getValue().source)
                    .get());
        }

    }
}
