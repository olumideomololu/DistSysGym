package org.example;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.nio.charset.StandardCharsets;

public class DistributedQueue {
    private static final Logger logger = Logger.getLogger(DistributedQueue.class.getName());

    record Message(
            String id,
            String payload,
            int partition,
            Instant createdAt,
            boolean processed
    ) {}

    static class Partition {
        private final int id;
        private final int maxSize;
        private final BlockingDeque<Message> messages;
        private final List<Partition> replicas;
        private final Lock lock;

        public Partition(int id, int maxSize) {
            this.id = id;
            this.maxSize = maxSize;
            this.messages = new LinkedBlockingDeque<>(maxSize);
            this.replicas = new ArrayList<>();
            this.lock = new ReentrantLock();
        }

        public void addReplica(Partition replica) {
            replicas.add(replica);
        }

        public CompletableFuture<Void> push(Message message) {
            return CompletableFuture.runAsync(() -> {
                try {
                    // Implement backpressure by blocking if queue is full
                    while (!messages.offer(message, 100, TimeUnit.MILLISECONDS)) {
                        // Keep trying
                    }

                    // Replicate to followers asynchronously
                    replicas.forEach(replica ->
                            CompletableFuture.runAsync(() -> replica.replicate(message))
                    );

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Push interrupted", e);
                }
            });
        }

        public void replicate(Message message) {
            lock.lock();
            try {
                messages.offer(message);
            } finally {
                lock.unlock();
            }
        }

        public CompletableFuture<Optional<Message>> pop() {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    Message message = messages.poll(100, TimeUnit.MILLISECONDS);
                    return Optional.ofNullable(message);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return Optional.empty();
                }
            });
        }
    }

    private final int numPartitions;
    private final int numReplicas;
    private final List<Partition> partitions;

    public DistributedQueue(int numPartitions, int numReplicas) {
        this.numPartitions = numPartitions;
        this.numReplicas = numReplicas;
        this.partitions = new ArrayList<>();
        setupPartitions();
    }

    private void setupPartitions() {
        // Create partitions
        for (int i = 0; i < numPartitions; i++) {
            partitions.add(new Partition(i, 1000));
        }

        // Setup replication
        for (int i = 0; i < partitions.size(); i++) {
            Partition partition = partitions.get(i);
            // Create replica partitions
            for (int r = 0; r < numReplicas; r++) {
                Partition replica = new Partition(i * 1000 + r, 1000);
                partition.addReplica(replica);
            }
        }
    }

    public int getPartition(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hashBytes = md.digest(key.getBytes(StandardCharsets.UTF_8));
            int hash = Math.abs(ByteBuffer.wrap(hashBytes).getInt());
            return hash % numPartitions;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not available", e);
        }
    }

    public CompletableFuture<Void> send(String key, String payload) {
        int partitionId = getPartition(key);
        Message message = new Message(
                key + "-" + Instant.now().toEpochMilli(),
                payload,
                partitionId,
                Instant.now(),
                false
        );

        logger.info("Sending message to partition " + partitionId + ": " + message);
        return partitions.get(partitionId).push(message);
    }

    public CompletableFuture<Optional<Message>> receive(int partitionId) {
        if (partitionId < partitions.size()) {
            return partitions.get(partitionId).pop()
                    .thenApply(messageOpt -> {
                        messageOpt.ifPresent(message ->
                                logger.info("Received message from partition " + partitionId + ": " + message)
                        );
                        return messageOpt;
                    });
        }
        return CompletableFuture.completedFuture(Optional.empty());
    }

    public static void main(String[] args) throws Exception {
        DistributedQueue queue = new DistributedQueue(3, 2);
        ExecutorService executorService = Executors.newFixedThreadPool(4);

        // Producer
        CompletableFuture<Void> producer = CompletableFuture.runAsync(() -> {
            try {
                for (int i = 0; i < 10; i++) {
                    String key = "user-" + (i % 5); // Using 5 different keys to distribute messages
                    queue.send(key, "Message " + i).join();
                    Thread.sleep(100); // Simulate some work
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // Consumers
        List<CompletableFuture<Void>> consumers = new ArrayList<>();
        for (int partitionId = 0; partitionId < 3; partitionId++) {
            final int pid = partitionId;
            consumers.add(CompletableFuture.runAsync(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        Optional<Message> message = queue.receive(pid).join();
                        if (message.isPresent()) {
                            // Simulate processing
                            Thread.sleep(200);
                            logger.info("Processed message: " + message.get());
                        } else {
                            Thread.sleep(100);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }

        // Wait for producer to finish
        producer.join();
        Thread.sleep(2000); // Allow time for remaining messages to be processed

        // Shutdown
        executorService.shutdownNow();
        executorService.awaitTermination(1, TimeUnit.SECONDS);
    }
}