package com.tom_e_white.drdobbs.mapreduce;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class TopKCollection {
  private int k;
  private List<Set<String>> wordSets = new ArrayList<Set<String>>();
  private List<Long> counts = new ArrayList<Long>();

  public TopKCollection(int k) {
    this.k = k;
  }

  public int getK() {
    return k;
  }

  public void add(String word, long count) {
    // if count < smallest, then return
    // if count in list then add and don't remove any
    // if count not in list then insert and remove tail
    int index = Collections.binarySearch(counts, count, Collections.reverseOrder());
    if (index >= 0) {
      wordSets.get(index).add(word);
    } else {
      int insertionPoint = -index - 1; // contract of binary search
      if (insertionPoint < k) { // only store if in top K
        Set<String> wordSet = new LinkedHashSet<String>();
        wordSet.add(word);
        wordSets.add(insertionPoint, wordSet);
        counts.add(insertionPoint, count);
        if (wordSets.size() == k + 1) {
          wordSets.remove(k);
          counts.remove(k);
        }
      }
    }
  }

  public List<Set<String>> getWordSets() {
    return wordSets;
  }

  public List<Long> getCounts() {
    return counts;
  }
}
