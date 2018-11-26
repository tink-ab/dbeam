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

package com.spotify.dbeam.avro;

import static com.spotify.dbeam.avro.JdbcAvroRecord.computeMapping;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

public class JdbcAvroRecordWriter {
  private final JdbcAvroRecord.SqlFunction<ResultSet, Object>[] mappings;
  private final int columnCount;
  private final DataFileWriter dataFileWriter;
  private final ResultSet resultSet;

  public JdbcAvroRecordWriter(
      JdbcAvroRecord.SqlFunction<ResultSet, Object>[] mappings, int columnCount,
      DataFileWriter dataFileWriter, ResultSet resultSet) {
    this.mappings = mappings;
    this.columnCount = columnCount;
    this.dataFileWriter = dataFileWriter;
    this.resultSet = resultSet;
  }

  public static JdbcAvroRecordWriter create(DataFileWriter dataFileWriter, ResultSet resultSet)
      throws SQLException {
    return new JdbcAvroRecordWriter(
        computeAllMappings(resultSet),
        resultSet.getMetaData().getColumnCount(),
        dataFileWriter,
        resultSet);
  }

  static JdbcAvroRecord.SqlFunction<ResultSet, Object>[] computeAllMappings(ResultSet resultSet)
      throws SQLException {
    final ResultSetMetaData meta = resultSet.getMetaData();
    final int columnCount = meta.getColumnCount();

    final JdbcAvroRecord.SqlFunction[] mappings =
        new JdbcAvroRecord.SqlFunction[columnCount + 1];

    for (int i = 1; i <= columnCount; i++) {
      mappings[i] = computeMapping(meta, i);
    }
    return (JdbcAvroRecord.SqlFunction<ResultSet, Object>[]) mappings;
  }

  public void writeSingleRecord() throws IOException, SQLException {
    final byte[] bytes = convertResultSetIntoAvroBytes();
    dataFileWriter.appendEncoded(ByteBuffer.wrap(bytes));
  }

  BinaryEncoder binaryEncoder = null;
  EncoderFactory encoderFactory = EncoderFactory.get();

  public static class MyByteArrayOutputStream extends ByteArrayOutputStream {

    public MyByteArrayOutputStream() {

    }

    public MyByteArrayOutputStream(int size) {
      super(size);
    }

    // provide access to internal buffer, avoiding copy
    public byte[] getBufffer() {
      return buf;
    }
  }

  byte[] convertResultSetIntoAvroBytes()
      throws SQLException, IOException {
    final MyByteArrayOutputStream out = new MyByteArrayOutputStream(columnCount * 64);
    binaryEncoder = encoderFactory.directBinaryEncoder(out, binaryEncoder);
    for (int i = 1; i <= columnCount; i++) {
      final Object value = mappings[i].apply(resultSet);
      if (value == null || resultSet.wasNull()) {
        binaryEncoder.writeIndex(0);
        binaryEncoder.writeNull();
      } else {
        binaryEncoder.writeIndex(1);
        if (value instanceof String) {
          binaryEncoder.writeString((String) value);
        } else if (value instanceof Long) {
          binaryEncoder.writeLong((Long) value);
        } else if (value instanceof Integer) {
          binaryEncoder.writeInt((Integer) value);
        } else if (value instanceof Boolean) {
          binaryEncoder.writeBoolean((Boolean) value);
        } else if (value instanceof ByteBuffer) {
          binaryEncoder.writeBytes((ByteBuffer) value);
        } else if (value instanceof Double) {
          binaryEncoder.writeDouble((Double) value);
        } else if (value instanceof Float) {
          binaryEncoder.writeFloat((Float) value);
        }
      }
    }
    binaryEncoder.flush();
    return out.getBufffer();
  }

}
