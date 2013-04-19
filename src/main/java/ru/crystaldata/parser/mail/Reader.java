package ru.crystaldata.parser.mail;

import com.mongodb.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.concurrent.*;

/**
 * User: eyakovleva
 * Date: 4/9/13
 * Time: 9:58 AM
 */
public class Reader {
    public static void main(String[] args) throws IOException, URISyntaxException {
        Mongo mongo = new Mongo("localhost", 27017);
        mongo.setWriteConcern(WriteConcern.NORMAL);
        DB database = mongo.getDB("mailru");
        DBCollection mailru = database.getCollection("catalogpages");
        final ConcurrentHashMap<String,String> results = new ConcurrentHashMap<String, String>();

        Files.walkFileTree(Paths.get("/home/ragvena/tinkoff/projects/processor/data/"), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                BufferedReader br = new BufferedReader(new FileReader(String.valueOf(file)));
                String currentLine;
                while ((currentLine=br.readLine())!=null){
                    String[] data = currentLine.split("\t");
                    results.put(data[0],"");
                }
                return FileVisitResult.CONTINUE;
            }
        });
        for(Map.Entry<String, String> entry: results.entrySet()){
           mailru.insert(new BasicDBObject("url", entry.getKey()));
        }

    }
}
