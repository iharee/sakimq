package com.arth.sakimq.broker.storage;

import com.arth.sakimq.broker.core.Message;
import com.arth.sakimq.exception.WalCloseException;
import com.arth.sakimq.exception.WalOpenException;
import com.arth.sakimq.exception.WalWriteException;
import com.arth.sakimq.protocol.WalRecord;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;


public final class FileWal implements MessageLog {

    private static final Logger log = LoggerFactory.getLogger(FileWal.class);

    private static final String WAL_PREFIX = "mq-";
    private static final String WAL_SUFFIX = ".wal";

    private final Path directory;
    private final Path path;
    private final FileOutputStream out;

    public FileWal(Path directory) {
        this.directory = directory;
        this.path = directory.resolve(WAL_PREFIX + System.currentTimeMillis() + WAL_SUFFIX);
        try {
            Files.createDirectories(directory);
            this.out = new FileOutputStream(path.toFile(), true);
        } catch (IOException e) {
            log.error("Failed to open WAL: {}", path, e);
            throw new WalOpenException("Failed to open WAL: " + path, e);
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
        log.debug("WAL append: type={}, queue={}, messageId={}, deliveryCount={}",
                record.getType(), record.getQueue(), record.getMessageId(), record.getDeliveryCount());
        try {
            record.writeDelimitedTo(out);
            out.flush();
            out.getFD().sync();  // fsync
        } catch (IOException e) {
            log.error("Failed to write WAL record: type={}, messageId={}", record.getType(), record.getMessageId(), e);
            throw new WalWriteException("Failed to write WAL record", e);
        }
    }

    /**
     * 扫描并重放数据目录下全部 WAL 段；若某段尾部存在崩溃时未写完的半条记录，将其截断。
     * 只能在 Broker 启动、开始接受请求之前调用。
     *
     * @see com.arth.sakimq.broker.storage.MessageLog#recover()
     */
    @Override
    public List<WalRecord> recover() {
        if (!Files.isDirectory(directory)) {
            log.debug("No WAL directory found: {}, nothing to recover", directory);
            return List.of();
        }
        List<WalRecord> records = new ArrayList<>();
        try (DirectoryStream<Path> stream =
                 Files.newDirectoryStream(directory, WAL_PREFIX + "*" + WAL_SUFFIX)) {
            List<Path> segments = new ArrayList<>();
            for (Path segment : stream) {
                segments.add(segment);
            }
            segments.sort(Comparator.comparing(p -> p.getFileName().toString()));
            for (Path segment : segments) {
                recoverSegment(segment, records);
            }
            log.info("WAL recovery complete: {} records from {} segment(s)", records.size(), segments.size());
        } catch (IOException e) {
            log.error("Failed to list WAL segments in directory: {}", directory, e);
        }
        return records;
    }

    private void recoverSegment(Path segment, List<WalRecord> records) {
        try (RandomAccessFile raf = new RandomAccessFile(segment.toFile(), "rw")) {
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
                    log.warn("Truncated torn WAL record in segment {}, truncating to last good offset {}",
                            segment.getFileName(), lastGoodOffset);
                    raf.setLength(lastGoodOffset);
                    break;
                }
                lastGoodOffset = raf.getFilePointer();
            }
        } catch (IOException e) {
            log.error("Failed to read WAL segment: {}", segment, e);
        }
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
            log.error("Failed to close WAL", e);
            throw new WalCloseException("Failed to close WAL", e);
        }
    }

    public Path path() {
        return path;
    }
}