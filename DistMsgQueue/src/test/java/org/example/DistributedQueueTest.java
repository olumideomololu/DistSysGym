package org.example;

import org.junit.jupiter.api.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class DistributedQueueTest {
    private DistributedQueue queue;
    private ExecutorService executorService;

    @BeforeEach
    void setUp() {
        queue = new DistributedQueue(3, 2);
        executorService = Executors.newFixedThreadPool(10);
    }

    @AfterEach
    void tearDown() {
        executorService.shutdownNow();
    }

    @Test
    @DisplayName("Messages with same key go to same partition")
    void testConsistentHashing() throws Exception {
        String key = "test-key";
        int expectedPartition = queue.getPartition(key);

        List<CompletableFuture<Void>> sends = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            sends.add(queue.send(key, "message-" + i));
        }

        // Wait for all sends to complete
        CompletableFuture.allOf(sends.toArray(new CompletableFuture[0])).join();

        // Collect messages from all partitions
        Map<Integer, List<DistributedQueue.Message>> messagesPerPartition = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            messagesPerPartition.put(i, new ArrayList<>());
            for (int j = 0; j < 100; j++) {
                Optional<DistributedQueue.Message> msg = queue.receive(i).join();
                int finalI = i;
                msg.ifPresent(m -> {
                    messagesPerPartition.get(finalI).add(m);
                });
            }
        }

        // Assert all messages went to expected partition
        assertEquals(100, messagesPerPartition.get(expectedPartition).size());
        for (int i = 0; i < 3; i++) {
            if (i != expectedPartition) {
                assertEquals(0, messagesPerPartition.get(i).size());
            }
        }
    }

    @Test
    @DisplayName("Test backpressure when queue is full")
    void testBackpressure() throws Exception {
        // Fill up a partition
        String key = "backpressure-test";
        int partition = queue.getPartition(key);
        CountDownLatch slowConsumerLatch = new CountDownLatch(1);

        // Start a slow consumer
        CompletableFuture<Void> consumer = CompletableFuture.runAsync(() -> {
            try {
                slowConsumerLatch.await();
                while (true) {
                    queue.receive(partition).join().ifPresent(msg -> {
                        try {
                            Thread.sleep(50); // Slow consumer
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // Measure time taken to send messages
        long startTime = System.nanoTime();
        List<CompletableFuture<Void>> sends = new ArrayList<>();
        for (int i = 0; i < 2000; i++) { // Try to send more messages than queue capacity
            sends.add(queue.send(key, "message-" + i));
        }
        slowConsumerLatch.countDown(); // Start consuming

        // Wait for all sends
        CompletableFuture.allOf(sends.toArray(new CompletableFuture[0])).join();
        long duration = System.nanoTime() - startTime;

        // Assert that it took significant time due to backpressure
        assertTrue(Duration.ofNanos(duration).toMillis() > 1000);
        consumer.cancel(true);
    }

    @Test
    @DisplayName("Test message replication to followers")
    void testReplication() throws Exception {
        // Send messages and verify they're replicated
        String key = "replication-test";
        int partition = queue.getPartition(key);

        // Send test messages
        List<CompletableFuture<Void>> sends = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            sends.add(queue.send(key, "message-" + i));
        }
        CompletableFuture.allOf(sends.toArray(new CompletableFuture[0])).join();

        // Verify messages in primary partition
        Set<String> primaryMessages = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            Optional<DistributedQueue.Message> msg = queue.receive(partition).join();
            assertTrue(msg.isPresent());
            primaryMessages.add(msg.get().payload());
        }

        // Verify replicas through reflection (for testing purposes)
        java.lang.reflect.Field partitionsField = DistributedQueue.class.getDeclaredField("partitions");
        partitionsField.setAccessible(true);
        List<DistributedQueue.Partition> partitions = (List<DistributedQueue.Partition>) partitionsField.get(queue);
        DistributedQueue.Partition primaryPartition = partitions.get(partition);

        java.lang.reflect.Field replicasField = DistributedQueue.Partition.class.getDeclaredField("replicas");
        replicasField.setAccessible(true);
        List<DistributedQueue.Partition> replicas = (List<DistributedQueue.Partition>) replicasField.get(primaryPartition);

        // Check each replica
        for (DistributedQueue.Partition replica : replicas) {
            Set<String> replicaMessages = new HashSet<>();
            java.lang.reflect.Field messagesField = DistributedQueue.Partition.class.getDeclaredField("messages");
            messagesField.setAccessible(true);
            BlockingDeque<DistributedQueue.Message> messages = (BlockingDeque<DistributedQueue.Message>) messagesField.get(replica);

            DistributedQueue.Message msg;
            while ((msg = messages.poll()) != null) {
                replicaMessages.add(msg.payload());
            }

            // Verify replica has same messages as primary
            assertEquals(primaryMessages, replicaMessages);
        }
    }

    @Test
    @DisplayName("Test concurrent producers and consumers")
    void testConcurrentOperations() throws Exception {
        int numProducers = 5;
        int numConsumers = 3;
        int messagesPerProducer = 1000;

        // Track all sent messages
        ConcurrentMap<String, String> sentMessages = new ConcurrentHashMap<>();
        ConcurrentMap<String, String> receivedMessages = new ConcurrentHashMap<>();
        CountDownLatch producerLatch = new CountDownLatch(numProducers);
        CountDownLatch consumerLatch = new CountDownLatch(numProducers * messagesPerProducer);

        // Start producers
        for (int p = 0; p < numProducers; p++) {
            final int producerId = p;
            CompletableFuture.runAsync(() -> {
                try {
                    for (int i = 0; i < messagesPerProducer; i++) {
                        String key = "producer-" + producerId + "-" + i;
                        String payload = "message-" + i;
                        queue.send(key, payload).join();
                        sentMessages.put(key, payload);
                    }
                } finally {
                    producerLatch.countDown();
                }
            }, executorService);
        }

        // Start consumers
        List<CompletableFuture<Void>> consumers = new ArrayList<>();
        for (int c = 0; c < numConsumers; c++) {
            final int partition = c;
            consumers.add(CompletableFuture.runAsync(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        queue.receive(partition).join().ifPresent(msg -> {
                            receivedMessages.put(msg.id(), msg.payload());
                            consumerLatch.countDown();
                        });
                    } catch (Exception e) {
                        break;
                    }
                }
            }, executorService));
        }

        // Wait for all producers to finish and all messages to be consumed
        assertTrue(producerLatch.await(30, TimeUnit.SECONDS));
        assertTrue(consumerLatch.await(30, TimeUnit.SECONDS));

        // Verify all sent messages were received
        assertEquals(sentMessages.size(), receivedMessages.size());
        sentMessages.forEach((key, value) ->
                assertTrue(receivedMessages.containsValue(value)));

        // Cleanup consumers
        consumers.forEach(c -> c.cancel(true));
    }
}