package com.arth.sakimq.broker.storage;

import com.arth.sakimq.broker.core.Message;
import com.arth.sakimq.protocol.WalRecord;
import com.google.protobuf.ByteString;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * 基于追加文件的 Write-Ahead Log，每次追加后立即 fsync
 */
public final class FileWal implements MessageLog {

    private final Path path;
    private final FileOutputStream out;

    public FileWal(Path path) {
        this.path = path;
        try {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            this.out = new FileOutputStream(path.toFile(), true);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open WAL: " + path, e);
        }
    }

    @Override
    public synchronized void appendPublish(Message message) {
        WalRecord record = WalRecord.newBuilder()
                .setType(WalRecord.Type.PUBLISH)
                .setQueue(message.queue())
                .setMessageId(message.messageId())
                .setBody(ByteString.copyFrom(message.body()))
                .setCreatedAt(message.createdAt())
                .build();
        writeRecord(record);
    }

    @Override
    public synchronized void appendAck(String queue, String messageId) {
        WalRecord record = WalRecord.newBuilder()
                .setType(WalRecord.Type.ACK)
                .setQueue(queue)
                .setMessageId(messageId)
                .build();
        writeRecord(record);
    }

    private void writeRecord(WalRecord record) {
        try {
            record.writeDelimitedTo(out);
            out.flush();
            out.getFD().sync();  // fsync
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public List<WalRecord> recover() {
        if (!Files.exists(path)) return List.of();
        List<WalRecord> records = new ArrayList<>();
        try (InputStream in = Files.newInputStream(path)) {
            WalRecord record;
            while ((record = WalRecord.parseDelimitedFrom(in)) != null) {
                records.add(record);
            }
        } catch (IOException e) {
            // 文件尾部可能是崩溃时未写完的半条记录，已解析部分仍然有效
        }
        return records;
    }

    @Override
    public void close() {
        try {
            out.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Path path() {
        return path;
    }
}
