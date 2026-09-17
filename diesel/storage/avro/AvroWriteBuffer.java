package diesel.storage.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class AvroWriteBuffer {
    private final List<Map<String, Object>> buffer;
    private final int bufferSize;
    private final AvroDataFileWriter dataFileWriter;
    private final ReentrantLock lock = new ReentrantLock();
    private volatile boolean closed = false;
    private int currentBufferSize = 0;

    public AvroWriteBuffer(List<String> columns, Map<String, Class<?>> columnTypes, File outputFile, int bufferSize) throws IOException {
        this.bufferSize = bufferSize;
        this.buffer = new ArrayList<>(bufferSize);
        this.dataFileWriter = new AvroDataFileWriter(columns, columnTypes, outputFile);
    }

    public void writeRow(Map<String, Object> row) throws IOException {
        if (closed) {
            throw new IOException("WriteBuffer is closed");
        }

        lock.lock();
        try {
            buffer.add(row);
            currentBufferSize++;
            
            if (currentBufferSize >= bufferSize) {
                flush();
            }
        } finally {
            lock.unlock();
        }
    }

    public void flush() throws IOException {
        if (closed || buffer.isEmpty()) {
            return;
        }

        lock.lock();
        try {
            for (Map<String, Object> row : buffer) {
                dataFileWriter.writeRow(row);
            }
            buffer.clear();
            currentBufferSize = 0;
            dataFileWriter.flush();
        } finally {
            lock.unlock();
        }
    }

    public void close() throws IOException {
        if (closed) {
            return;
        }

        lock.lock();
        try {
            flush();
            dataFileWriter.close();
            closed = true;
        } finally {
            lock.unlock();
        }
    }

    public void rollback() throws IOException {
        if (closed) {
            return;
        }

        lock.lock();
        try {
            buffer.clear();
            currentBufferSize = 0;
            dataFileWriter.rollback();
            closed = true;
        } finally {
            lock.unlock();
        }
    }

    public int getCurrentBufferSize() {
        return currentBufferSize;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public boolean isClosed() {
        return closed;
    }
}