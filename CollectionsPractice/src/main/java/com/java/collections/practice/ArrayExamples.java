package com.java.collections.practice;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by chas6003 on 17-04-2019.
 */
public class ArrayExamples {
    public static void main(String[] args) {

        // List Examples
        // 1. ArrayList
        /***************************************************************************/
        List<String> strList = new ArrayList<String>();
        strList.add("one");
        strList.add("two");
        strList.add("three");
        strList.add("one");
        strList.add(null);

        System.out.println("Str List :: " + strList);
        // After removing one element using the object name
        strList.remove("one");
        System.out.println("Str List :: " + strList);

        // After removing one element using the index
        strList.remove(2);
        System.out.println("Str List :: " + strList);

        // Printing all the elements of Array List
        Iterator<String> iter = strList.iterator();
        while (iter.hasNext()) {
            System.out.println(iter.next());
        }
        /***************************************************************************/

        // 2. LinkedList
        List<Integer> strList2 = new LinkedList<Integer>();
        strList2.add(0,1);
        strList2.add(1,2);
        strList2.add(2,3);
        strList2.add(3,4);
        strList2.add(2, 2);
        Iterator<Integer> iter2 = strList2.iterator();
        while(iter2.hasNext()) {
            System.out.println(iter2.next());
        }

        List<String> strList3 = Arrays.asList("one", "two", "three");
        Iterator<String> iter3 = strList3.iterator();
        while(iter3.hasNext()) {
            System.out.println(iter3.next());
        }
        /***************************************************************************/

        // 3. HashSet - Integers

        Set<String> intSet1 = new HashSet<String>();
        intSet1.add("Ravi");
        intSet1.add("Vijay");
        intSet1.add("Ravi");
        intSet1.add("Ajay");
        intSet1.add("Sanjay");

        Iterator<String> intIter = intSet1.iterator();
        while (intIter.hasNext()) {
            System.out.println(intIter.next());
        }

        // LinkedHashset - Strings
        Set<String> strSet1 = new LinkedHashSet<String>();
        strSet1.add("Ravi");
        strSet1.add("Vijay");
        strSet1.add("Ravi");
        strSet1.add("Ajay");
        strSet1.add("Sanjay");

        Iterator<String> setIter2 = strSet1.iterator();
        while (setIter2.hasNext()) {
            System.out.println(setIter2.next());
        }

        // Treeset - Integers
        Set<Integer> treeSet1 = new TreeSet<Integer>();
        treeSet1.add(3);
        treeSet1.add(1);
        treeSet1.add(4);
        treeSet1.add(5);
        treeSet1.add(2);

        Iterator<Integer> treeSetiter1 = treeSet1.iterator();
        while (treeSetiter1.hasNext()) {
            System.out.println(treeSetiter1.next());
        }

        // HashMap - (Integer -> String)
        Map<Integer, String> map1 = new HashMap<Integer, String>();
        map1.put(1, "ashok");
        map1.put(3, "choppadandi");
        map1.put(2, "kumar");

        for (Map.Entry m  : map1.entrySet()) {
            System.out.println(m.getKey() + " -> " + m.getValue());
        }

        Set<Integer> keys = map1.keySet();
        Iterator<Integer> keysIter = keys.iterator();
        while (keysIter.hasNext()) {
            Integer key = keysIter.next();
            System.out.println(key + " -> " + map1.get(key));
        }

        System.out.println("Is ashok exists in map1 ? :: " + map1.containsValue("ashok"));
        System.out.println("The value of key = 4 is value =  " + map1.get(4));

        Set<Map.Entry<Integer, String>> entrySet = map1.entrySet();
        Iterator<Map.Entry<Integer, String>> entryIterator = entrySet.iterator();

        while (entryIterator.hasNext()) {
            Map.Entry<Integer, String> entry = entryIterator.next();
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }

        // LinkedHashMap - (Integer -> String)
        Map<Integer, String> map2 = new LinkedHashMap<Integer, String>();
        map2.put(1, "ashok");
        map2.put(3, "choppadandi");
        map2.put(2, "kumar");

        for(Map.Entry m : map2.entrySet()) {
            System.out.println(m.getKey() + " -> " + m.getValue());
        }

        // TreeMap - (Integer -> String)
        SortedMap<Integer, String> map3 = new TreeMap<Integer, String>();
        map3.put(1, "ashok");
        map3.put(3, "choppadandi");
        map3.put(2, "kumar");

        for(Map.Entry m : map3.entrySet()) {
            System.out.println(m.getKey() + " -> " + m.getValue());
        }
    }
}
