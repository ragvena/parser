package ru.mail.utils;

import org.htmlcleaner.TagNode;
import org.htmlcleaner.conditional.ITagNodeCondition;

/**
 * User: eyakovleva
 * Date: 4/5/13
 * Time: 4:35 PM
 */
public class NodeCondition {
   public static final ITagNodeCondition URL_CONDITION = new ITagNodeCondition() {
        @Override
        public boolean satisfy(TagNode tagNode) {
            if (tagNode.getName().equals("a")) {
                String href = tagNode.getAttributeByName("href");
                if (href != null && href.length() > 0) {
                    if (href.contains("site_jump.bat")) {
                        if (tagNode.getText().length() > 5 && tagNode.getText().subSequence(0, 4).equals("http"))
                            return true;
                    }
                }
            }
            return false;

        }
    };
}
