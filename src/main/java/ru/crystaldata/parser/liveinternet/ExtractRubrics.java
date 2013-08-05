package ru.crystaldata.parser.liveinternet;

import com.mongodb.*;
import ru.crystaldata.parser.common.Domain;
import ru.crystaldata.parser.common.RecordField;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

/**
 * User: eyakovleva
 * Date: 4/19/13
 * Time: 8:36 AM
 */
public class ExtractRubrics {
    public static void main(String[] args) throws IOException {
        Mongo mongo = new Mongo("localhost");

        DB rubricatorDatabase = mongo.getDB("rubricator");
        DBCollection liveinternet = rubricatorDatabase.getCollection("normalized");
        DBCursor cursor = liveinternet.find();
        PrintWriter pw = new PrintWriter(new FileOutputStream("rubricator"));
        while (cursor.hasNext()){
            DBObject domainInfo = cursor.next();
            String domain = (String) domainInfo.get(RecordField.DOMAIN);
            List<String> rubrics = (List<String>) domainInfo.get(RecordField.RUBRIC);
            Iterator<String> rubricsIterator = rubrics.iterator();
            StringBuilder builder = new StringBuilder();
            while (rubricsIterator.hasNext()){
                builder.append(rubricsIterator.next());
                if (rubricsIterator.hasNext()){
                    builder.append(":");
                }
            }
            pw.println(domain+"\t"+builder.toString());
        }
        pw.close();


//        DBCursor rubricatorCursor = liveinternet.find();
//        Set<String> rubrics = new HashSet<String>();
//        while (rubricatorCursor.hasNext()){
//            DBObject current = rubricatorCursor.next();
//            Map<String, Double> r = (Map<String, Double>) current.get(RecordField.RUBRIC);
//            for(Map.Entry<String, Double> entity: r.entrySet()){
//                rubrics.add(entity.getKey().split("/")[1]);
//            }
//        }
//        List<String> ordered = new LinkedList<String>(rubrics);
//        Collections.sort(ordered);
//        for(String entity: ordered){
//            System.out.println(entity);
//        }
    }
}
