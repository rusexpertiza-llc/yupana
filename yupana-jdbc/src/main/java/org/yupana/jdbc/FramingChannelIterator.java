/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yupana.jdbc;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;

public class FramingChannelIterator implements Iterator<byte[]> {

    public FramingChannelIterator(ReadableByteChannel channel, int frameSize) {
        this.frameSize = frameSize;
        this.channel = channel;
    }

    private final int frameSize;
    private final ReadableByteChannel channel;

    private final ByteBuffer buffer = createBuffer();

    private final byte[] intArray = new byte[4];
    private final ByteBuffer intBuffer = ByteBuffer.wrap(intArray);

    private boolean atEnd = false;

    private ByteBuffer createBuffer() {
        ByteBuffer buf = ByteBuffer.allocate(frameSize).order(ByteOrder.BIG_ENDIAN);
        buf.flip();
        return buf;
    }

    @Override
    public boolean hasNext() {
        if (channel.isOpen() && buffer.remaining() == 0) {
            try {
                fetch(1);
            } catch (IOException e) {
                throw new IllegalStateException("Unable to fetch data");
            }
        }
        return !atEnd && channel.isOpen();
    }

    @Override
    public byte[] next() {
        if (!hasNext()) throw new IllegalStateException("Next is called on empty iterator");
        byte[] result = new byte[0];
        try {
            read(intArray, 0);

            intBuffer.clear();
            int chunkSize = intBuffer.getInt();

            result = new byte[chunkSize];
            read(result, 0);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    private int read(byte[] dst, int offset) throws IOException {
        int bytesRequired = dst.length - offset;
        int totalRead = fetch(bytesRequired);
        buffer.get(dst, offset, Math.min(totalRead, bytesRequired));

        if (totalRead < bytesRequired && !atEnd) {
            buffer.clear();
            buffer.flip();
            totalRead += read(dst, totalRead);
        }

        if (totalRead < bytesRequired)
            throw new IllegalStateException(String.format("%d bytes requested but got only %d", bytesRequired, totalRead));

        return totalRead;
    }

    private int fetch(int size) throws IOException {
        int lastRead = 0;
        int totalRead = buffer.remaining();
        buffer.mark();
        buffer.position(buffer.limit());
        buffer.limit(buffer.capacity());
        while (totalRead < size && buffer.remaining() > 0 && lastRead != -1) {
            lastRead = channel.read(buffer);
            if (lastRead != -1) {
                totalRead += lastRead;
            }
        }

        buffer.limit(buffer.position());
        buffer.reset();
        atEnd = lastRead == -1;
        return totalRead;
    }

}
