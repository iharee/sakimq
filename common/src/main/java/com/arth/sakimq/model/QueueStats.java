package com.arth.sakimq.model;

public record QueueStats(
        String queue,
        long readyCount,
        long inflightCount,
        long totalCount
) {

    public static QueueStats empty(String queue) {
        return new QueueStats(queue, 0, 0, 0);
    }
}
