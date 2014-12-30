package test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.hadoop.ParquetReader;
import parquet.proto.ProtoReadSupport;
import tachyon.hadoop.TFS;

public class TestReader {
  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.tachyon.impl", TFS.class.getName());
    ProtoReadSupport.setRequestedProjection(conf,
        "message TestSchema {optional int32 f1;}");

    ParquetReader<TestSchema.Test> r =
        ParquetReader
            .builder(new ProtoReadSupport<TestSchema.Test>(),
                new Path("tachyon://localhost:19998/outfile")).withConf(conf).build();

    Object t = r.read();
    while (t != null) {
      System.out.println(((TestSchema.Test.Builder) t).build());
      t = r.read();
    }
  }
}
