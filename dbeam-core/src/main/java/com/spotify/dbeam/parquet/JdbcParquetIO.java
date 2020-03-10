/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.dbeam.parquet;

import com.google.common.collect.ImmutableMap;
import com.spotify.dbeam.args.JdbcAvroArgs;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.DynamicAvroDestinations;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.ShardNameTemplate;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PCollection;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JdbcParquetIO {

  private static final String DEFAULT_SHARD_TEMPLATE = ShardNameTemplate.INDEX_OF_MAX;

  public static PTransform<PCollection<String>, WriteFilesResult<Void>> createWrite(
      String filenamePrefix, String filenameSuffix, Schema schema,
      JdbcAvroArgs jdbcAvroArgs) {
    filenamePrefix = filenamePrefix.replaceAll("/+$", "") + "/part";
    ValueProvider<ResourceId> prefixProvider =
        StaticValueProvider.of(FileBasedSink.convertToFileResourceIfPossible(filenamePrefix));
    FileBasedSink.FilenamePolicy filenamePolicy =
        DefaultFilenamePolicy.fromStandardParameters(
            prefixProvider,
            DEFAULT_SHARD_TEMPLATE,
            filenameSuffix,
            false);

    final DynamicAvroDestinations<String, Void, String>
        destinations =
        AvroIO.constantDestinations(filenamePolicy, schema, ImmutableMap.of(),
                                    jdbcAvroArgs.getCodecFactory(),
                                    SerializableFunctions.identity());
    final FileBasedSink<String, Void, String> sink = new JdbcAvroSink<>(
        prefixProvider,
        destinations,
        jdbcAvroArgs);
    return WriteFiles.to(sink);
  }


  static class JdbcAvroSink<UserT> extends FileBasedSink<UserT, Void, String> {

    private final DynamicAvroDestinations<?, Void, String> dynamicDestinations;
    private final JdbcAvroArgs jdbcAvroArgs;

    JdbcAvroSink(ValueProvider<ResourceId> filenamePrefix,
                 DynamicAvroDestinations<UserT, Void, String> dynamicDestinations,
                 JdbcAvroArgs jdbcAvroArgs) {
      super(filenamePrefix, dynamicDestinations, Compression.UNCOMPRESSED);
      this.dynamicDestinations = dynamicDestinations;
      this.jdbcAvroArgs = jdbcAvroArgs;
    }

    @Override
    public WriteOperation<Void, String> createWriteOperation() {
      return new JdbcAvroWriteOperation(this, dynamicDestinations, jdbcAvroArgs);
    }
  }

  private static class JdbcAvroWriteOperation
      extends FileBasedSink.WriteOperation<Void, String> {

    private final DynamicAvroDestinations<?, Void, String> dynamicDestinations;
    private final JdbcAvroArgs jdbcAvroArgs;

    private JdbcAvroWriteOperation(FileBasedSink<?, Void, String> sink,
                                   DynamicAvroDestinations<?, Void, String> dynamicDestinations,
                                   JdbcAvroArgs jdbcAvroArgs) {

      super(sink);
      this.dynamicDestinations = dynamicDestinations;
      this.jdbcAvroArgs = jdbcAvroArgs;
    }

    @Override
    public FileBasedSink.Writer<Void, String> createWriter() {
      return new JdbcAvroWriter(this, dynamicDestinations, jdbcAvroArgs);
    }
  }

  private static class JdbcAvroWriter extends FileBasedSink.Writer<Void, String> {
    private final Logger logger = LoggerFactory.getLogger(JdbcAvroWriter.class);
    private final DynamicAvroDestinations<?, Void, String> dynamicDestinations;
    private final JdbcAvroArgs jdbcAvroArgs;
    private Connection connection;
    private JdbcParquetMetering metering;
    private ParquetWriter<GenericRecord> parquetWriter;

    JdbcAvroWriter(FileBasedSink.WriteOperation<Void, String> writeOperation,
                   DynamicAvroDestinations<?, Void, String> dynamicDestinations,
                   JdbcAvroArgs jdbcAvroArgs) {
      super(writeOperation, MimeTypes.BINARY);
      this.dynamicDestinations = dynamicDestinations;
      this.jdbcAvroArgs = jdbcAvroArgs;
    }

    public Void getDestination() {
      return null;
    }

    @SuppressWarnings("deprecation") // uses internal test functionality.
    @Override
    protected void prepareWrite(WritableByteChannel channel) throws Exception {
      logger.info("jdbcavroio : Preparing write...");
      connection = jdbcAvroArgs.jdbcConnectionConfiguration().createConnection();
      Void destination = getDestination();
      CodecFactory codec = dynamicDestinations.getCodec(destination);
      Schema schema = dynamicDestinations.getSchema(destination);

      BeamParquetOutputFile beamParquetOutputFile = new BeamParquetOutputFile(Channels.newOutputStream(channel));
       parquetWriter = AvroParquetWriter.<GenericRecord>builder(beamParquetOutputFile)
              .withSchema(schema)
              .withCompressionCodec(CompressionCodecName.SNAPPY)
              .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
              .build();

      this.metering = JdbcParquetMetering.create();
      logger.info("jdbcavroio : Write prepared");
    }

    private ResultSet executeQuery(String query) throws Exception {
      checkArgument(connection != null,
                    "JDBC connection was not properly created");
      PreparedStatement statement = connection.prepareStatement(
          query,
          ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.CONCUR_READ_ONLY);
      statement.setFetchSize(jdbcAvroArgs.fetchSize());
      if (jdbcAvroArgs.statementPreparator() != null) {
        jdbcAvroArgs.statementPreparator().setParameters(statement);
      }

      long startTime = System.currentTimeMillis();
      logger.info(
          "jdbcavroio : Executing query with fetchSize={} (this might take a few minutes) ...",
          statement.getFetchSize());
      ResultSet resultSet = statement.executeQuery();
      this.metering.finishExecuteQuery(System.currentTimeMillis() - startTime);
      return resultSet;
    }

    @Override
    public void write(String query) throws Exception {
      checkArgument(parquetWriter != null,
                    "Avro DataFileWriter was not properly created");
      logger.info("jdbcavroio : Starting write...");
      Schema schema = dynamicDestinations.getSchema(getDestination());
      try (ResultSet resultSet = executeQuery(query)) {
        checkArgument(resultSet != null,
                      "JDBC resultSet was not properly created");
        final Map<Integer, JdbcParquetRecord.SqlFunction<ResultSet, Object>>
            mappings = JdbcParquetRecord.computeAllMappings(resultSet);
        final int columnCount = resultSet.getMetaData().getColumnCount();
        this.metering.startIterate();
        while (resultSet.next()) {
          final GenericRecord genericRecord =
              JdbcParquetRecord.convertResultSetIntoAvroRecord(
                  schema, resultSet, mappings, columnCount);
          this.parquetWriter.write(genericRecord);
          this.metering.incrementRecordCount();
        }
        this.metering.finishIterate();
      }
    }

    @Override
    protected void finishWrite() throws Exception {
      logger.info("jdbcavroio : Closing connection, flushing writer...");
      if (connection != null) {
        connection.close();
      }
      parquetWriter.close();
      logger.info("jdbcavroio : Write finished");
    }

    private static class BeamParquetOutputFile implements OutputFile {

      private OutputStream outputStream;

      BeamParquetOutputFile(OutputStream outputStream) {
        this.outputStream = outputStream;
      }

      @Override
      public PositionOutputStream create(long blockSizeHint) {
        return new BeamOutputStream(outputStream);
      }

      @Override
      public PositionOutputStream createOrOverwrite(long blockSizeHint) {
        return new BeamOutputStream(outputStream);
      }

      @Override
      public boolean supportsBlockSize() {
        return false;
      }

      @Override
      public long defaultBlockSize() {
        return 0;
      }
    }

    private static class BeamOutputStream extends PositionOutputStream {
      private long position = 0;
      private OutputStream outputStream;

      private BeamOutputStream(OutputStream outputStream) {
        this.outputStream = outputStream;
      }

      @Override
      public long getPos() throws IOException {
        return position;
      }

      @Override
      public void write(int b) throws IOException {
        position++;
        outputStream.write(b);
      }

      @Override
      public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        outputStream.write(b, off, len);
        position += len;
      }

      @Override
      public void flush() throws IOException {
        outputStream.flush();
      }

      @Override
      public void close() throws IOException {
        outputStream.close();
      }
    }
  }


}
