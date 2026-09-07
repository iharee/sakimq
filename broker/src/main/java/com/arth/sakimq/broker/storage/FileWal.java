package com.arth.sakimq.broker.storage;

import com.arth.sakimq.broker.core.MessageRecord;
import com.arth.sakimq.protocol.WalRecord;

import java.nio.file.Path;
import java.util.List;

public final class FileWal implements MessageLog {

    private final Path path;

    public FileWal(Path path) {
        this.path = path;
    }

    @Override
    public void appendPublish(MessageRecord message) {
    }

    @Override
    public void appendAck(String queue, String receiptHandle) {
    }

    @Override
    public List<WalRecord> recover() {
        return List.of();
    }

    public Path path() {
        return path;
    }

    @Override
    public void close() {
    }
}
