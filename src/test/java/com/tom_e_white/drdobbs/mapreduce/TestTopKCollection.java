package com.tom_e_white.drdobbs.mapreduce;

import com.google.common.base.Joiner;
import org.junit.Test;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class TestTopKCollection {
  @Test
  public void test() throws Exception {
    TopKCollection topK = new TopKCollection(2);
    topK.add("cat", 6);
    assertCounts(topK, 6);
    assertWords(topK, "cat");

    topK.add("dog", 4);
    assertCounts(topK, 6, 4);
    assertWords(topK, "cat", "dog");

    topK.add("bee", 9);
    assertCounts(topK, 9, 6);
    assertWords(topK, "bee", "cat");

    topK.add("pig", 9);
    assertCounts(topK, 9, 6);
    assertWords(topK, "bee,pig", "cat");

    topK.add("cow", 10);
    assertCounts(topK, 10, 9);
    assertWords(topK, "cow", "bee,pig");
  }

  private void assertCounts(TopKCollection topK, long... expectedCounts) {
    List<Long> actualCounts = topK.getCounts();
    assertEquals("Size of counts; actual " + actualCounts + ", expected: " + Arrays.asList(expectedCounts), expectedCounts.length, actualCounts.size());
    for (int i = 0; i < expectedCounts.length; i++) {
      assertEquals("Count", expectedCounts[i], actualCounts.get(i).longValue());
    }
  }

  private void assertWords(TopKCollection topK, String... expectedWords) {
    List<Set<String>> actualWords = topK.getWordSets();
    assertEquals("Size of words", expectedWords.length, actualWords.size());
    for (int i = 0; i < expectedWords.length; i++) {
      assertEquals("Word", expectedWords[i], Joiner.on(",").join(actualWords.get(i)));
    }
  }
}
