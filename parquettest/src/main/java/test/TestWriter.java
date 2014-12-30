package test;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.hadoop.ParquetWriter;
import parquet.proto.ProtoWriteSupport;
import tachyon.hadoop.TFS;

public class TestWriter {
  public static void main(String[] args) throws IOException, URISyntaxException {
    Configuration conf = new Configuration();
    conf.set("fs.tachyon.impl", TFS.class.getName());
    ParquetWriter<TestSchema.Test> w =
        new ParquetWriter<TestSchema.Test>(new Path("tachyon://localhost:19998/outfile"),
            new ProtoWriteSupport<TestSchema.Test>(TestSchema.Test.class),
            ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME, ParquetWriter.DEFAULT_BLOCK_SIZE,
            ParquetWriter.DEFAULT_PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE,
            ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
            ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED, ParquetWriter.DEFAULT_WRITER_VERSION, conf);

    for (int i = 0; i < 100; ++i) {
      w.write(TestSchema.Test.newBuilder().setF1(i).setF2((float) i).setF3(String.valueOf(i))
          .build());
    }
    w.close();
  }
}
