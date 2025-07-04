/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
   SeekableByteArrayInputStream copied from
   https://github.com/apache/accumulo/blob/master/core/src/test/java/org/apache/accumulo/core/file/rfile/RFileTest.java
 */

package io.github.spark_redshift_community.spark.redshift.test;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.ByteArrayInputStream;
import java.io.IOException;


class SeekableByteArrayInputStream extends ByteArrayInputStream
        implements Seekable, PositionedReadable {

    public SeekableByteArrayInputStream(byte[] buf) {
        super(buf);
    }

    @Override
    public long getPos() {
        return pos;
    }

    @Override
    public void seek(long pos) throws IOException {
        if (mark != 0)
            throw new IllegalStateException();

        reset();
        long skipped = skip(pos);

        if (skipped != pos)
            throw new IOException();
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
        return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) {

        if (position >= buf.length)
            throw new IllegalArgumentException();
        if (position + length > buf.length)
            throw new IllegalArgumentException();
        if (length > buffer.length)
            throw new IllegalArgumentException();

        System.arraycopy(buf, (int) position, buffer, offset, length);
        return length;
    }

    @Override
    public void readFully(long position, byte[] buffer) {
        read(position, buffer, 0, buffer.length);

    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) {
        read(position, buffer, offset, length);
    }

}

