package ru.crystaldata.parser.common;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.*;

/**
 * User: eyakovleva
 * Date: 4/19/13
 * Time: 9:49 AM
 */
public class Merger {
    public static void main(String[] args) throws UnknownHostException {
        Mongo mongo = new Mongo("localhost");
        DB rubricatorDatabase = mongo.getDB("rubricator");
        DBCollection yandex = rubricatorDatabase.getCollection("yandexcatalog");
        DBCollection mail = rubricatorDatabase.getCollection("mail");
        DBCollection liveinternet = rubricatorDatabase.getCollection("liveinternet");
        DBCollection rubricator = rubricatorDatabase.getCollection("rubricator");
        Map<String, Domain> result = new HashMap<String, Domain>();
        System.out.println("Start merging");
        addDomainInfoFromCollection(yandex, result);
        System.out.println("Merge Yandex finish\t" + result.size());
        addDomainInfoFromCollection(mail, result);
        System.out.println("Merge Mail finish\t"+result.size());
        addDomainInfoFromCollection(liveinternet, result);
        System.out.println("Merge LiveInternet finish\t"+result.size());
        for(Map.Entry<String, Domain> entity: result.entrySet()) {
            rubricator.insert(BasicDBObjectBuilder.start()
                    .add(RecordField.DOMAIN, entity.getKey())
                    .add(RecordField.RUBRIC, entity.getValue().rubric)
                    .add(RecordField.SOURCE, entity.getValue().source)
                    .get());
        }

    }

    private static Map<String, Domain> addDomainInfoFromCollection(DBCollection collection, Map<String, Domain> data){
        DBCursor cursor = collection.find();
        while (cursor.hasNext()){
            DBObject  current = cursor.next();
            String domain  = (String) current.get(RecordField.DOMAIN);
            Map<String, Double> rubric = (Map<String, Double>) current.get(RecordField.RUBRIC);
            List<String> source = (List<String>) current.get(RecordField.SOURCE);
            Domain currentDomainInfo = new Domain(new HashSet<String>(source), rubric);
            if (data.containsKey(domain)){
                currentDomainInfo.merge(data.get(domain));
            }
            data.put(domain, currentDomainInfo);

        }
        return  data;
    }
}
