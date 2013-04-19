package ru.crystaldata.parser.common;

import java.util.*;

/**
 * User: eyakovleva
 * Date: 4/19/13
 * Time: 8:45 AM
 */
public class Domain {
    public Set<String> source;
    public Map<String, Double> rubric;
    private final static Double DEFAULT_RUBRIC_VALUE = 1.0;

    public Domain(Set<String> source, Map<String, Double> rubric) {
        this.source = source;
        this.rubric = rubric;
    }



    public Domain(Set<String> source, String rubrics) {
        this.source =source;
        rubric = new HashMap<String, Double>();
        rubric.put(rubrics, DEFAULT_RUBRIC_VALUE);
    }

    public Domain() {
        source = new HashSet<String>();
        rubric = new HashMap<String, Double>();
    }

    public void addRubrics(Map<String, Double> additionRubric) {
        for (Map.Entry<String, Double> entity : additionRubric.entrySet()) {
            Double counter = entity.getValue();
            if (rubric.containsKey(entity.getKey())) {
                counter += rubric.get(entity.getKey());
            }
            rubric.put(entity.getKey(), counter);
        }
    }

    public void merge(Domain additionInfo){
        this.source.addAll(additionInfo.source);
        addRubrics(additionInfo.rubric);
    }


}
