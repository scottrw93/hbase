package org.apache.hadoop.hbase.mapreduce.replication.filter;

public interface RowFilter {
  boolean include(byte[] row);
}
