package com.amazonaws.glue.catalog.converters;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * A column that is all NULL has no min/max. Writers such as Trino leave lowValue/highValue unset in that case,
 * and Glue's MinimumValue/MaximumValue are optional, so both directions must convert without a NullPointerException.
 */
public class ColumnStatisticsNullMinMaxTest {

  private final CatalogToHiveConverter catalogToHiveConverter = new Hive3CatalogToHiveConverter();

  @Test
  public void testDateStatsWithoutMinMax() {
    DateColumnStatsData hiveDate = new DateColumnStatsData(5, 0);  // numNulls, numDVs; low/high unset
    ColumnStatistics hiveStats = tableStats("d", "date", ColumnStatisticsData.dateStats(hiveDate));

    List<com.amazonaws.services.glue.model.ColumnStatistics> catalogStats =
        HiveToCatalogConverter.convertColumnStatisticsObjList(hiveStats);
    com.amazonaws.services.glue.model.DateColumnStatisticsData catalogDate =
        catalogStats.get(0).getStatisticsData().getDateColumnStatisticsData();
    assertNull(catalogDate.getMinimumValue());
    assertNull(catalogDate.getMaximumValue());
    assertEquals(Long.valueOf(5), catalogDate.getNumberOfNulls());

    DateColumnStatsData roundTrip = catalogToHiveConverter.convertColumnStatisticsList(catalogStats)
        .get(0).getStatsData().getDateStats();
    assertFalse(roundTrip.isSetLowValue());
    assertFalse(roundTrip.isSetHighValue());
    assertEquals(5, roundTrip.getNumNulls());
  }

  @Test
  public void testDecimalStatsWithoutMinMax() {
    DecimalColumnStatsData hiveDecimal = new DecimalColumnStatsData(5, 0);  // numNulls, numDVs; low/high unset
    ColumnStatistics hiveStats = tableStats("v", "decimal(18,4)", ColumnStatisticsData.decimalStats(hiveDecimal));

    List<com.amazonaws.services.glue.model.ColumnStatistics> catalogStats =
        HiveToCatalogConverter.convertColumnStatisticsObjList(hiveStats);
    com.amazonaws.services.glue.model.DecimalColumnStatisticsData catalogDecimal =
        catalogStats.get(0).getStatisticsData().getDecimalColumnStatisticsData();
    assertNull(catalogDecimal.getMinimumValue());
    assertNull(catalogDecimal.getMaximumValue());
    assertEquals(Long.valueOf(5), catalogDecimal.getNumberOfNulls());

    DecimalColumnStatsData roundTrip = catalogToHiveConverter.convertColumnStatisticsList(catalogStats)
        .get(0).getStatsData().getDecimalStats();
    assertFalse(roundTrip.isSetLowValue());
    assertFalse(roundTrip.isSetHighValue());
    assertEquals(5, roundTrip.getNumNulls());
  }

  private static ColumnStatistics tableStats(String column, String type, ColumnStatisticsData data) {
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc(true, "db", "tbl");
    desc.setLastAnalyzed(0);
    return new ColumnStatistics(desc, ImmutableList.of(new ColumnStatisticsObj(column, type, data)));
  }
}
