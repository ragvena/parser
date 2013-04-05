package ru.mail;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataAnalyzer {
    private static Pattern domainPattern = Pattern.compile("^http[^:]{0,1}://(www\\.){0,1}([^/]+).*");

    private DataAnalyzer() {
    }

    public static String getUrlDomain(String url) {
        Matcher matcher = domainPattern.matcher(url);
        if (matcher.find()) {
            return matcher.group(2);
        } else {
            System.out.println("Bad urls\t" + url);
            return "";

        }
    }

    public static void main(String[] args) {
    }


}
