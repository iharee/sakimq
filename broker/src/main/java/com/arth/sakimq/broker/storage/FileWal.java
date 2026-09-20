package com.arth.sakimq.broker.storage;

import com.arth.sakimq.broker.core.Message;
import com.arth.sakimq.protocol.WalRecord;
import com.google.protobuf.ByteString;

import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
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

    @Override
    public synchronized void appendDelivery(String queue, String messageId, int deliveryCount) {
        WalRecord record = WalRecord.newBuilder()
                .setType(WalRecord.Type.DELIVERY)
                .setQueue(queue)
                .setMessageId(messageId)
                .setDeliveryCount(deliveryCount)
                .build();
        writeRecord(record);
    }

    @Override
    public synchronized void appendCreateQueue(String queue) {
        WalRecord record = WalRecord.newBuilder()
                .setType(WalRecord.Type.CREATE_QUEUE)
                .setQueue(queue)
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

    /**    (non-Javadoc)
     * 扫描并重放全部有效记录；若尾部存在崩溃时未写完的半条记录，将其截断。只能在 Broker 启动、开始接受请求之前调用。
     * 
     * @see com.arth.sakimq.broker.storage.MessageLog#recover()
     */
    @Override
    public List<WalRecord> recover() {
        if (!Files.exists(path)) return List.of();
        List<WalRecord> records = new ArrayList<>();
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
            long lastGoodOffset = 0;
            while (true) {
                long recordStart = raf.getFilePointer();
                if (recordStart >= raf.length()) {
                    break;
                }
                try {
                    int size = readRawVarint32(raf);
                    if (size < 0 || raf.getFilePointer() + size > raf.length()) {
                        throw new EOFException("truncated WAL record");
                    }
                    byte[] data = new byte[size];
                    raf.readFully(data);
                    records.add(WalRecord.parseFrom(data));
                } catch (IOException e) {
                    // 半条记录，恢复至上一条完好记录
                    raf.setLength(lastGoodOffset);
                    break;
                }
                lastGoodOffset = raf.getFilePointer();
            }
        } catch (IOException e) {
            // 文件不可读
        }
        return records;
    }


    /**
     * 读取与 writeDelimitedTo 一致的 protobuf varint 长度前缀
     * 
     * @param in
     * @return protobuf varint 长度前缀
     * @throws IOException
     */
    private static int readRawVarint32(RandomAccessFile in) throws IOException {
        int result = 0;
        for (int shift = 0; shift < 32; shift += 7) {
            int b = in.readUnsignedByte();
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new IOException("malformed varint");
    }

    @Override
    public synchronized void close() {
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
