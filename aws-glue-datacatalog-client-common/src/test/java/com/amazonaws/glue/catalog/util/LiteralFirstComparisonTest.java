package com.amazonaws.glue.catalog.util;

import org.junit.Test;

import static com.amazonaws.glue.catalog.util.ExpressionHelper.moveLiteralsToRightOfComparisons;
import static com.amazonaws.glue.catalog.util.ExpressionHelper.quoteDateAndTimestampLiterals;
import static com.amazonaws.glue.catalog.util.ExpressionHelper.replaceDoubleQuoteWithSingleQuotes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class LiteralFirstComparisonTest {

  @Test
  public void rewritesTheFilterSparkSendsForLiteralOnTheLeft() {
    String sparkFilter = "\"2026-10-04\" <= acquisition_date and acquisition_date < \"2026-10-15\"";

    assertEquals("acquisition_date >= '2026-10-04' and acquisition_date < '2026-10-15'",
        toGlueFilter(sparkFilter));
  }

  @Test
  public void mirrorsEveryOrderingOperator() {
    assertEquals("c > 1", moveLiteralsToRightOfComparisons("1 < c"));
    assertEquals("c >= 1", moveLiteralsToRightOfComparisons("1 <= c"));
    assertEquals("c < 1", moveLiteralsToRightOfComparisons("1 > c"));
    assertEquals("c <= 1", moveLiteralsToRightOfComparisons("1 >= c"));
  }

  @Test
  public void keepsSymmetricOperators() {
    assertEquals("c = 'a'", moveLiteralsToRightOfComparisons("'a' = c"));
    assertEquals("c <> 'a'", moveLiteralsToRightOfComparisons("'a' <> c"));
    assertEquals("c != 'a'", moveLiteralsToRightOfComparisons("'a' != c"));
  }

  @Test
  public void handlesNumericLiterals() {
    assertEquals("hour >= -1", moveLiteralsToRightOfComparisons("-1 <= hour"));
    assertEquals("ratio < 0.5", moveLiteralsToRightOfComparisons("0.5 > ratio"));
  }

  @Test
  public void rewritesLiteralFirstComparisonsInsideOrGroupsAndAfterNot() {
    assertEquals("(c > '2026-10-04' or d = 'x')",
        moveLiteralsToRightOfComparisons("('2026-10-04' < c or 'x' = d)"));
    assertEquals("not c = 'x'", moveLiteralsToRightOfComparisons("not 'x' = c"));
  }

  @Test
  public void quotesBareDateLiteralsOnceMovedToTheRight() {
    assertEquals("event_date >= '2026-02-02'", toGlueFilter("2026-02-02 <= event_date"));
  }

  @Test
  public void leavesColumnFirstFiltersUntouched() {
    String filter = "acquisition_date >= '2026-10-04' and acquisition_date < '2026-10-15'";

    assertEquals(filter, moveLiteralsToRightOfComparisons(filter));
  }

  @Test
  public void isIdempotent() {
    String once = moveLiteralsToRightOfComparisons("'2026-10-04' <= d and d < '2026-10-15'");

    assertEquals(once, moveLiteralsToRightOfComparisons(once));
  }

  @Test
  public void doesNotTouchComparisonTextInsideQuotedValues() {
    String filter = "name = 'a and 1 < b' and c = 'x'";

    assertEquals(filter, moveLiteralsToRightOfComparisons(filter));
  }

  @Test
  public void leavesLiteralFirstFunctionCallsAlone() {
    String filter = "'x' = upper(c)";

    assertEquals(filter, moveLiteralsToRightOfComparisons(filter));
  }

  @Test
  public void handlesNullAndEmpty() {
    assertNull(moveLiteralsToRightOfComparisons(null));
    assertEquals("", moveLiteralsToRightOfComparisons(""));
  }

  private static String toGlueFilter(String filter) {
    String singleQuoted = replaceDoubleQuoteWithSingleQuotes(filter);
    return quoteDateAndTimestampLiterals(moveLiteralsToRightOfComparisons(singleQuoted));
  }
}
