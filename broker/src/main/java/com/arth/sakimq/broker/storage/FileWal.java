package com.arth.sakimq.broker.storage;

import com.arth.sakimq.broker.core.Message;
import com.arth.sakimq.exception.WalCloseException;
import com.arth.sakimq.exception.WalOpenException;
import com.arth.sakimq.exception.WalRecoveryException;
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
    /** 38b5f7b 之前的单文件 WAL，恢复时作为最老的一段处理 */
    private static final String LEGACY_WAL_NAME = "mq.wal";
    /**
     * 单条记录上限，防止损坏的长度前缀导致超大数组分配。
     * 写入侧同样受此限制，因此超限记录只可能来自更早的版本（那时没有上限），恢复时会 fail closed 拒绝启动；
     * 这是刻意的兼容性边界：唯一的替代上界是文件长度，等于允许损坏的长度前缀按文件大小分配数组。
     */
    private static final int MAX_WAL_RECORD_SIZE = 64 * 1024 * 1024;

    private final Path directory;
    private final Path path;
    private final FileOutputStream out;

    public FileWal(Path directory) {
        this.directory = directory;
        this.path = directory.resolve(WAL_PREFIX + nextSegmentTimestamp(directory) + WAL_SUFFIX);
        try {
            Files.createDirectories(directory);
            this.out = new FileOutputStream(path.toFile(), true);
        } catch (IOException e) {
            log.error("Failed to open WAL: {}", path, e);
            throw new WalOpenException("Failed to open WAL: " + path, e);
        }
    }

    /**
     * 生成严格大于所有既有 segment 的时间戳，保证恢复时按文件名字典序排序等同于写入顺序。
     * 不直接使用墙钟，避免系统时间回拨导致新 segment 排在旧 segment 之前（跨段 PUBLISH/ACK 顺序颠倒）。
     */
    private static long nextSegmentTimestamp(Path directory) {
        long max = System.currentTimeMillis();
        if (Files.isDirectory(directory)) {
            try (DirectoryStream<Path> stream =
                     Files.newDirectoryStream(directory, WAL_PREFIX + "*" + WAL_SUFFIX)) {
                for (Path segment : stream) {
                    String name = segment.getFileName().toString();
                    String ts = name.substring(WAL_PREFIX.length(), name.length() - WAL_SUFFIX.length());
                    try {
                        max = Math.max(max, Long.parseLong(ts));
                    } catch (NumberFormatException ignored) {
                        // 非时间戳命名的历史段，忽略
                    }
                }
            } catch (IOException e) {
                log.error("Failed to scan existing WAL segments in directory: {}", directory, e);
                throw new WalOpenException("Failed to scan existing WAL segments in directory: " + directory, e);
            }
        }
        return max + 1;
    }

    /**
     * 校验记录的业务不变量，用以检查数据完整性
     */
    private static void validateRecord(WalRecord record) {
        if (record.getType() == WalRecord.Type.UNRECOGNIZED) {
            throw new WalRecoveryException("Unknown WAL record type: " + record.getTypeValue());
        }
        requireNonBlank(record.getQueue(), "queue", record);
        switch (record.getType()) {
            case PUBLISH, ACK, DELIVERY, DISCARD -> {
                requireNonBlank(record.getMessageId(), "messageId", record);
                if (record.getType() == WalRecord.Type.DELIVERY && record.getDeliveryCount() <= 0) {
                    throw new WalRecoveryException("WAL DELIVERY record with non-positive deliveryCount: "
                            + record.getDeliveryCount());
                }
            }
            case DEAD_LETTER -> {
                requireNonBlank(record.getMessageId(), "messageId", record);
                requireNonBlank(record.getTargetQueue(), "targetQueue", record);
                if (record.getTargetQueue().equals(record.getQueue())) {
                    throw new WalRecoveryException("WAL DEAD_LETTER record whose targetQueue equals its queue: "
                            + record.getQueue());
                }
            }
            default -> {
            }
        }
    }

    private static void requireNonBlank(String value, String field, WalRecord record) {
        if (value == null || value.isBlank()) {
            throw new WalRecoveryException("WAL " + record.getType() + " record with blank " + field);
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

    @Override
    public synchronized void appendDeadLetter(String queue, String messageId, String targetQueue) {
        WalRecord record = WalRecord.newBuilder()
                .setType(WalRecord.Type.DEAD_LETTER)
                .setQueue(queue)
                .setMessageId(messageId)
                .setTargetQueue(targetQueue)
                .build();
        writeRecord(record);
    }

    @Override
    public synchronized void appendDiscard(String queue, String messageId) {
        WalRecord record = WalRecord.newBuilder()
                .setType(WalRecord.Type.DISCARD)
                .setQueue(queue)
                .setMessageId(messageId)
                .build();
        writeRecord(record);
    }

    private void writeRecord(WalRecord record) {
        log.debug("WAL append: type={}, queue={}, messageId={}, deliveryCount={}",
                record.getType(), record.getQueue(), record.getMessageId(), record.getDeliveryCount());
        // 与 recoverSegment 的上限保持一致：写得下去却读不回来的记录会让 broker 重启后 fail closed 拒绝启动
        int size = record.getSerializedSize();
        if (size > MAX_WAL_RECORD_SIZE) {
            throw new WalWriteException("WAL record size " + size + " exceeds limit " + MAX_WAL_RECORD_SIZE);
        }
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
     * 扫描并重放数据目录下全部 WAL 段。只有崩溃时正在写入的那一段（最后一个非空段）
     * 才可能留下未写完的半条记录，此时截断到上一条完好记录；更早的段出现半条记录
     * 说明是记录损坏而非崩溃残留，截断会静默删除其后的有效记录，一律 fail closed。
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
            // 兼容 38b5f7b 之前的单文件 WAL：文件名不含分隔符，不匹配 mq-*.wal，需显式恢复且恒为最老的一段
            Path legacy = directory.resolve(LEGACY_WAL_NAME);
            if (Files.isRegularFile(legacy)) {
                log.info("Found legacy WAL file {}, recovering as oldest segment", legacy);
                segments.add(legacy);
            }
            List<Path> named = new ArrayList<>();
            for (Path segment : stream) {
                named.add(segment);
            }
            named.sort(Comparator.comparing(p -> p.getFileName().toString()));
            segments.addAll(named);

            // 只有崩溃时正在写入的那一段（最后一个非空段）才可能留下未写完的半条记录。
            // 更早的段出现半条记录说明是记录损坏而非崩溃残留，截断会静默删除其后的有效记录，必须 fail closed
            Path active = lastNonEmptySegment(segments);
            for (Path segment : segments) {
                recoverSegment(segment, records, segment.equals(active));
            }
            log.info("WAL recovery complete: {} records from {} segment(s)", records.size(), segments.size());
        } catch (IOException e) {
            // 目录存在却无法列出属于错误，不得带着部分数据启动
            log.error("Failed to list WAL segments in directory: {}", directory, e);
            throw new WalRecoveryException("Failed to list WAL segments in directory: " + directory, e);
        }
        return records;
    }

    /**
     * @param segments 按时间顺序排列的段
     * @return 最后一个非空段，即崩溃时正在写入的那一段；全部为空时返回 null
     */
    private static Path lastNonEmptySegment(List<Path> segments) throws IOException {
        for (int i = segments.size() - 1; i >= 0; i--) {
            if (Files.size(segments.get(i)) > 0) {
                return segments.get(i);
            }
        }
        return null;
    }

    private void recoverSegment(Path segment, List<WalRecord> records, boolean truncatable) {
        try (RandomAccessFile raf = new RandomAccessFile(segment.toFile(), "rw")) {
            long lastGoodOffset = 0;
            while (true) {
                long recordStart = raf.getFilePointer();
                if (recordStart >= raf.length()) {
                    break;
                }
                try {
                    int size = readRawVarint32(raf);
                    // 长度前缀本身非法（0 / 负数 / 超上限）：正确写入的记录不可能为空，也不超过上限，
                    // 因此属于损坏而非崩溃残留，不能落到下面按 torn tail 截断的分支
                    if (size <= 0 || size > MAX_WAL_RECORD_SIZE) {
                        throw new IOException("invalid WAL record size: " + size);
                    }
                    if (raf.getFilePointer() + size > raf.length()) {
                        throw new EOFException("truncated WAL record");
                    }
                    byte[] data = new byte[size];
                    raf.readFully(data);
                    WalRecord record = WalRecord.parseFrom(data);
                    validateRecord(record);
                    records.add(record);
                } catch (EOFException e) {
                    if (!truncatable) {
                        // 非活动段出现半条记录：截断会连同其后的有效记录一起静默删除，fail closed
                        log.error("Torn WAL record in non-active segment {} at offset {}, refusing to truncate",
                                segment.getFileName(), recordStart);
                        throw new WalRecoveryException("Torn WAL record in non-active segment "
                                + segment.getFileName() + " at offset " + recordStart, e);
                    }
                    // 崩溃时未写完的半条记录（长度前缀或内容读到文件末尾），恢复至上一条完好记录
                    log.warn("Truncated torn WAL record in segment {}, truncating to last good offset {}",
                            segment.getFileName(), lastGoodOffset);
                    raf.setLength(lastGoodOffset);
                    break;
                } catch (IOException e) {
                    // 完整记录但内容损坏，或真实 I/O 错误：不能截断，否则会静默丢失后续有效记录，fail closed
                    log.error("Corrupt WAL record in segment {} at offset {}, refusing to truncate: {}",
                            segment.getFileName(), recordStart, e.toString());
                    throw new WalRecoveryException(
                            "Corrupt WAL record in segment " + segment.getFileName() + " at offset " + recordStart, e);
                }
                lastGoodOffset = raf.getFilePointer();
            }
        } catch (IOException e) {
            // 段文件无法打开/读取：同样 fail closed，避免静默丢失整段数据
            log.error("Failed to read WAL segment: {}", segment, e);
            throw new WalRecoveryException("Failed to read WAL segment: " + segment, e);
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
            // 长度必须非负，第 5 个字节只能贡献 bits 28-30（payload <= 0x07）；
            // 0x08 起会把符号位置 1 变成负数，0x10 起或仍带 continuation bit 均超出 32 位，一律视为损坏
            if (shift == 28 && (b & 0xF8) != 0) {
                throw new IOException("malformed varint");
            }
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
