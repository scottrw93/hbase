package org.apache.hadoop.hbase.filter;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

public class BenchmarkFilterDeserialization {
  private static final FilterProtos.Filter SIMPLE;
  private static final FilterProtos.Filter FILTER;
  static {
    LinkedList<Filter> list = new LinkedList<>();
    list.add(new ColumnCountGetFilter(1));
    list.add(new RowFilter(CompareOperator.EQUAL,
        new SubstringComparator("testFilterList")));

    try {
      SIMPLE = ProtobufUtil.toFilter(new FilterList(new LinkedList<>(list)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    LinkedList<Filter> innerList = new LinkedList<>();
    innerList.add(new FamilyFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("test"))));
    innerList.add(new SingleColumnValueFilter(Bytes.toBytes("a"), Bytes.toBytes("b"), CompareOperator.EQUAL, Bytes.toBytes("c")));
    FilterList inner = new FilterList(innerList);
    list.add(inner);
    FilterProtos.Filter filter = null;
    try {
      FILTER = ProtobufUtil.toFilter(new FilterList(list));
    } catch (IOException e) {
     throw new RuntimeException(e);
    }
  }

  @State(Scope.Benchmark)
  public static class ExecutionPlan {

    @Param({"true", "false"})
    public boolean useCreator;


    @Setup(Level.Invocation)
    public void setUp() {
      ProtobufUtil.USE_CREATOR.set(useCreator);
    }
  }

  @Benchmark
  @Fork(value = 1, warmups = 1)
  @BenchmarkMode({Mode.AverageTime})
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void test(Blackhole blackhole, ExecutionPlan executionPlan) throws IOException {
    blackhole.consume(ProtobufUtil.toFilter(FILTER, executionPlan.useCreator));
  }

  @Benchmark
  @Fork(value = 1, warmups = 1)
  @BenchmarkMode({Mode.AverageTime})
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void testSimple(Blackhole blackhole, ExecutionPlan executionPlan) throws IOException {
    blackhole.consume(ProtobufUtil.toFilter(SIMPLE, executionPlan.useCreator));
  }

//  @Benchmark
//  @Fork(value = 1, warmups = 1)
//  @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
//  @OutputTimeUnit(TimeUnit.NANOSECONDS)
//  public void testWithoutCreator(Blackhole blackhole) throws IOException {
//    blackhole.consume(ProtobufUtil.toFilter(FILTER, false));
//  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
