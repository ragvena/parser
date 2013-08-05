package ru.crystaldata.parser.yandex;

import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;

import java.io.*;
import java.net.URL;
import java.util.*;

/**
 * User: eyakovleva
 * Date: 4/29/13
 * Time: 2:47 PM
 */
public class RequestParser {
    public static final String PREFIX = "http://yaca.yandex.ru/yca/cat/?text=";

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("/home/ragvena/Downloads/unknown.csv"));
        String currentDomain;
        Map<String, String> rubrics = new HashMap<String, String>();
        Integer count = 0;
        String defaultSource = "yandex";

        while ((currentDomain = br.readLine()) != null) {
            HtmlCleaner cleaner = new HtmlCleaner();
            count++;
            TagNode root = cleaner.clean(new URL(PREFIX + currentDomain), "utf-8");
            List<TagNode> resultItems = root.getElementListByAttValue("class", "b-result__region", true, false);
            if (resultItems != null && resultItems.size() > 1) {
                Set<String> rawRubrics = new HashSet<String>();
                for(TagNode node:resultItems){
                   if(node.getAttributeByName("href").contains("/yca/cat")){
                       rawRubrics.add(node.getText().toString());
                   }
                }

                rubrics.put(currentDomain, setToString(rawRubrics));
            }
            System.out.println(count + "\t" + rubrics.size());
        }
        PrintWriter pw = new PrintWriter(new FileWriter("/home/ragvena/Downloads/additional.csv"));
        for (Map.Entry<String, String> data : rubrics.entrySet()) {
            pw.println(data.getKey() + "\t" + data.getValue() + "\t" + defaultSource);
        }
        pw.close();
    }

    private static String setToString(Set<String> list) {
        StringBuilder builder = new StringBuilder("");
        for (String entity : list) {
            if (builder.length() > 0) {
                builder.append(",");
            }
            builder.append(entity);
        }
        return builder.toString();
    }
}
