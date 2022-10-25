package org.apache.hadoop.hbase.mapreduce.replication.filter;

public class PassThroughFilter implements RowFilter {
  private static final PassThroughFilter FILTER = new PassThroughFilter();

  @Override
  public boolean include(byte[] row) {
    return true;
  }

  public static RowFilter impl() {
    return FILTER;
  }
}
