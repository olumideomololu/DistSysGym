package org.example;

import java.util.*;
import java.util.concurrent.*;

/**
 * DistributedKeyValueStore.java
 * <p>
 * This is a simplified simulation of a distributed key-value store using
 * the Raft consensus protocol. Each node runs in its own thread, communicates
 * via in-memory message queues (simulating network messages), and supports basic
 * Raft RPCs: RequestVote and AppendEntries (which serves both as heartbeat and
 * log replication). The leader accepts client commands (of the form "put key value")
 * and replicates them to the followers. Once a log entry is committed (i.e. replicated to
 * a majority), it is applied to each node’s state machine (a simple Map).
 * <p>
 * Note: This code is for educational/demo purposes and omits many production details.
 */
public class DistributedKeyValueStore {

    // --- Raft Role Definitions ---
    enum Role {
        LEADER, FOLLOWER, CANDIDATE
    }

    // --- Log Entry Definition ---
    /**
     * LogEntry represents one command entry in the replicated log.
     * It contains the term when the entry was received and a command string (e.g., "put key value").
     */
    static class LogEntry {
        int term;          // The term when this entry was created.
        String command;    // The command to apply to the state machine.

        public LogEntry(int term, String command) {
            this.term = term;
            this.command = command;
        }

        @Override
        public String toString() {
            return "[term:" + term + ", cmd:" + command + "]";
        }
    }

    // --- Raft Message Definitions ---
    /**
     * Base class for all Raft RPC messages.
     */
    static abstract class RaftMessage {
        int term;      // Sender’s current term.
        int senderId;  // The ID of the sender node.

        public RaftMessage(int term, int senderId) {
            this.term = term;
            this.senderId = senderId;
        }
    }

    /**
     * RequestVote RPC used during elections.
     */
    static class RequestVote extends RaftMessage {
        int lastLogIndex; // Index of candidate's last log entry.
        int lastLogTerm;  // Term of candidate's last log entry.

        public RequestVote(int term, int senderId, int lastLogIndex, int lastLogTerm) {
            super(term, senderId);
            this.lastLogIndex = lastLogIndex;
            this.lastLogTerm = lastLogTerm;
        }
    }

    /**
     * Response for RequestVote RPC.
     */
    static class RequestVoteResponse extends RaftMessage {
        boolean voteGranted; // true if the vote is granted.

        public RequestVoteResponse(int term, int senderId, boolean voteGranted) {
            super(term, senderId);
            this.voteGranted = voteGranted;
        }
    }

    /**
     * AppendEntries RPC used for both heartbeats and log replication.
     */
    static class AppendEntries extends RaftMessage {
        int prevLogIndex;       // Index of log entry immediately preceding new ones.
        int prevLogTerm;        // Term of prevLogIndex entry.
        List<LogEntry> entries; // Log entries to store (empty for heartbeat).
        int leaderCommit;       // Leader’s commit index.

        public AppendEntries(int term, int senderId, int prevLogIndex, int prevLogTerm,
                             List<LogEntry> entries, int leaderCommit) {
            super(term, senderId);
            this.prevLogIndex = prevLogIndex;
            this.prevLogTerm = prevLogTerm;
            this.entries = entries;
            this.leaderCommit = leaderCommit;
        }
    }

    /**
     * Response for AppendEntries RPC.
     */
    static class AppendEntriesResponse extends RaftMessage {
        boolean success; // true if follower contained entry matching prevLogIndex and prevLogTerm.
        int matchIndex;  // The index of the last entry matched (for optimizations).

        public AppendEntriesResponse(int term, int senderId, boolean success, int matchIndex) {
            super(term, senderId);
            this.success = success;
            this.matchIndex = matchIndex;
        }
    }

    // --- Raft Node Implementation ---
    /**
     * RaftNode represents one node in the cluster. Each node maintains its own log,
     * current term, vote state, state machine (a simple key-value store), and runs its
     * own main loop to process incoming messages.
     */
    static class RaftNode extends Thread {
        int id;                            // Unique identifier for the node.
        volatile int currentTerm = 0;      // Latest term seen.
        volatile Integer votedFor = null;  // CandidateID that received vote in current term.
        List<LogEntry> log = new ArrayList<>(); // The replicated log.
        volatile int commitIndex = -1;     // Highest log entry known to be committed.
        volatile int lastApplied = -1;     // Highest log entry applied to the state machine.
        Map<String, String> stateMachine = new ConcurrentHashMap<>(); // The key-value store.

        volatile Role role = Role.FOLLOWER;  // The node’s role.
        List<RaftNode> peers = new ArrayList<>(); // Other nodes in the cluster.

        // Queue for simulating incoming network messages.
        BlockingQueue<RaftMessage> messageQueue = new LinkedBlockingQueue<>();

        // Random generator for randomized election timeouts.
        Random random = new Random();

        // Flag to control the node’s main loop.
        volatile boolean running = true;

        // Scheduler for election timeouts and heartbeat tasks.
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        ScheduledFuture<?> electionTimeoutTask;
        ScheduledFuture<?> heartbeatTask;

        // Used to count votes during an election.
        final Object voteLock = new Object();
        int voteCount = 0; // Votes received in the current election.

        public RaftNode(int id) {
            this.id = id;
        }

        /**
         * Set the peer nodes for this Raft node.
         */
        public void setPeers(List<RaftNode> peers) {
            this.peers = peers;
        }

        /**
         * Reset the election timeout.
         * Cancels any existing timeout and schedules a new one with a randomized delay.
         */
        private void resetElectionTimeout() {
            if (electionTimeoutTask != null && !electionTimeoutTask.isCancelled()) {
                electionTimeoutTask.cancel(true);
            }
            // For simulation, choose a timeout between 1500 and 3000 ms.
            int timeout = 1500 + random.nextInt(1500);
            electionTimeoutTask = scheduler.schedule(() -> {
                // If no heartbeat received, start an election.
                if (role != Role.LEADER) {
                    startElection();
                }
            }, timeout, TimeUnit.MILLISECONDS);
        }

        /**
         * The main loop for the Raft node.
         * It polls for incoming messages and applies committed log entries.
         */
        public void run() {
            // Start as follower and schedule an election timeout.
            role = Role.FOLLOWER;
            resetElectionTimeout();

            while (running) {
                try {
                    // Poll for a message (with timeout so we can check periodically).
                    RaftMessage msg = messageQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        processMessage(msg);
                    }
                    // Apply any committed but not yet applied log entries.
                    applyLogEntries();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            scheduler.shutdownNow();
        }

        /**
         * Process an incoming Raft RPC message.
         */
        private void processMessage(RaftMessage msg) {
            if (msg instanceof RequestVote) {
                handleRequestVote((RequestVote) msg);
            } else if (msg instanceof RequestVoteResponse) {
                handleRequestVoteResponse((RequestVoteResponse) msg);
            } else if (msg instanceof AppendEntries) {
                handleAppendEntries((AppendEntries) msg);
            } else if (msg instanceof AppendEntriesResponse) {
                handleAppendEntriesResponse((AppendEntriesResponse) msg);
            }
        }

        /**
         * Handle a RequestVote RPC.
         * Votes for the candidate if (a) the candidate’s term is at least as high,
         * (b) this node hasn’t voted for another candidate in this term, and
         * (c) the candidate’s log is at least as up-to-date.
         */
        private void handleRequestVote(RequestVote rv) {
            // Reject the request if its term is older.
            if (rv.term < currentTerm) {
                sendMessage(rv.senderId, new RequestVoteResponse(currentTerm, id, false));
                return;
            }
            // If the candidate’s term is higher, update our term and become follower.
            if (rv.term > currentTerm) {
                currentTerm = rv.term;
                role = Role.FOLLOWER;
                votedFor = null;
            }
            // Check if we can vote for this candidate.
            boolean voteGranted = false;
            if ((votedFor == null || votedFor == rv.senderId) &&
                    isCandidateLogUpToDate(rv.lastLogIndex, rv.lastLogTerm)) {
                voteGranted = true;
                votedFor = rv.senderId;
                // Reset election timeout when granting vote.
                resetElectionTimeout();
            }
            sendMessage(rv.senderId, new RequestVoteResponse(currentTerm, id, voteGranted));
        }

        /**
         * Check whether the candidate’s log is at least as up-to-date as this node’s log.
         */
        private boolean isCandidateLogUpToDate(int candidateLastLogIndex, int candidateLastLogTerm) {
            int lastLogIndex = log.size() - 1;
            int lastLogTerm = lastLogIndex >= 0 ? log.get(lastLogIndex).term : 0;
            if (candidateLastLogTerm != lastLogTerm) {
                return candidateLastLogTerm > lastLogTerm;
            }
            return candidateLastLogIndex >= lastLogIndex;
        }

        /**
         * Handle the response from a RequestVote RPC.
         * If the node is a candidate and receives a majority of votes, it becomes the leader.
         */
        private void handleRequestVoteResponse(RequestVoteResponse rvr) {
            // Only candidates process vote responses.
            if (role != Role.CANDIDATE) {
                return;
            }
            // If the responder’s term is higher, update our term and become follower.
            if (rvr.term > currentTerm) {
                currentTerm = rvr.term;
                role = Role.FOLLOWER;
                votedFor = null;
                resetElectionTimeout();
                return;
            }
            // Count the vote if it is granted.
            if (rvr.voteGranted) {
                synchronized (voteLock) {
                    voteCount++;
                    // If a majority is reached (including self), become leader.
                    if (voteCount > (peers.size() + 1) / 2) { // total nodes = peers + self
                        becomeLeader();
                    }
                }
            }
        }

        /**
         * Transition from candidate to leader.
         * Cancels election timeouts and begins sending periodic heartbeats.
         */
        private void becomeLeader() {
            role = Role.LEADER;
            System.out.println("Node " + id + " became LEADER for term " + currentTerm);
            if (electionTimeoutTask != null) {
                electionTimeoutTask.cancel(true);
            }
            // In a complete implementation, the leader would initialize nextIndex and matchIndex for each follower.
            // Start a heartbeat task to send empty AppendEntries RPCs.
            heartbeatTask = scheduler.scheduleAtFixedRate(this::sendHeartbeat, 0, 500, TimeUnit.MILLISECONDS); // heartbeat every 500ms
        }

        /**
         * Send heartbeat messages (empty AppendEntries RPCs) to all peers.
         */
        private void sendHeartbeat() {
            for (RaftNode peer : peers) {
                int prevLogIndex = log.size() - 1;
                int prevLogTerm = prevLogIndex >= 0 ? log.get(prevLogIndex).term : 0;
                AppendEntries heartbeat = new AppendEntries(currentTerm, id,
                        prevLogIndex, prevLogTerm,
                        new ArrayList<>(), commitIndex);
                sendMessage(peer.id, heartbeat);
            }
        }

        /**
         * Handle an AppendEntries RPC. This method works for both heartbeats and log replication.
         */
        private void handleAppendEntries(AppendEntries ae) {
            // Reject if the leader’s term is older.
            if (ae.term < currentTerm) {
                sendMessage(ae.senderId, new AppendEntriesResponse(currentTerm, id, false, -1));
                return;
            }
            // If the term is higher, update our term and become follower.
            if (ae.term > currentTerm) {
                currentTerm = ae.term;
                role = Role.FOLLOWER;
                votedFor = null;
            }
            // Reset election timeout since a valid leader contacted us.
            resetElectionTimeout();

            // Check that the log contains an entry at prevLogIndex with matching term.
            if (ae.prevLogIndex >= 0) {
                if (log.size() - 1 < ae.prevLogIndex ||
                        log.get(ae.prevLogIndex).term != ae.prevLogTerm) {
                    // If not, reply with failure.
                    sendMessage(ae.senderId, new AppendEntriesResponse(currentTerm, id, false, -1));
                    return;
                }
            }
            // Append any new entries (or overwrite conflicting entries).
            int index = ae.prevLogIndex + 1;
            for (LogEntry entry : ae.entries) {
                if (log.size() > index) {
                    // Conflict: delete the existing entry and all that follow.
                    if (log.get(index).term != entry.term) {
                        log = new ArrayList<>(log.subList(0, index));
                        log.add(entry);
                    }
                } else {
                    log.add(entry);
                }
                index++;
            }
            // Update commit index to the minimum of leader’s commit and our last log index.
            if (ae.leaderCommit > commitIndex) {
                commitIndex = Math.min(ae.leaderCommit, log.size() - 1);
            }
            // Reply success with the index of the last log entry.
            sendMessage(ae.senderId, new AppendEntriesResponse(currentTerm, id, true, log.size() - 1));
        }

        /**
         * Handle the response for AppendEntries RPC.
         * (In this simplified implementation we do not update nextIndex/matchIndex.)
         */
        private void handleAppendEntriesResponse(AppendEntriesResponse aer) {
            // Only the leader should handle these responses.
            if (role != Role.LEADER) return;
            // If the follower’s term is higher, step down.
            if (aer.term > currentTerm) {
                currentTerm = aer.term;
                role = Role.FOLLOWER;
                votedFor = null;
                if (heartbeatTask != null) {
                    heartbeatTask.cancel(true);
                }
                resetElectionTimeout();
            }
            // In a full implementation, the leader would use these responses to update its replication state.
        }

        /**
         * Submit a client command (e.g., "put key value") to this node.
         * Only the leader should accept client commands.
         */
        public void submitCommand(String command) {
            if (role != Role.LEADER) {
                System.out.println("Node " + id + " is not the leader. Rejecting command: " + command);
                return;
            }
            // Append the command to the leader’s log.
            LogEntry entry = new LogEntry(currentTerm, command);
            log.add(entry);
            int entryIndex = log.size() - 1;

            // Broadcast an AppendEntries RPC containing the new log entry.
            for (RaftNode peer : peers) {
                int prevLogIndex = entryIndex - 1;
                int prevLogTerm = prevLogIndex >= 0 ? log.get(prevLogIndex).term : 0;
                List<LogEntry> entries = new ArrayList<>();
                entries.add(entry);
                AppendEntries ae = new AppendEntries(currentTerm, id, prevLogIndex, prevLogTerm, entries, commitIndex);
                sendMessage(peer.id, ae);
            }
            // In a complete Raft implementation the leader would wait for acknowledgments from a majority.
            // Here, for simplicity, we simulate commitment after a fixed delay.
            scheduler.schedule(() -> {
                commitIndex = entryIndex;
                applyLogEntries(); // Apply the newly committed entry.
            }, 500, TimeUnit.MILLISECONDS);
        }

        /**
         * Apply committed but not yet applied log entries to the state machine.
         * For our key-value store, we process "put key value" commands.
         */
        private void applyLogEntries() {
            while (lastApplied < commitIndex) {
                lastApplied++;
                LogEntry entry = log.get(lastApplied);
                // Parse the command.
                String[] parts = entry.command.split(" ");
                if (parts.length == 3 && parts[0].equalsIgnoreCase("put")) {
                    String key = parts[1];
                    String value = parts[2];
                    stateMachine.put(key, value);
                    System.out.println("Node " + id + " applied command: " + entry.command);
                }
                // (Other command types can be handled here.)
            }
        }

        /**
         * Utility method to send a message to a peer identified by peerId.
         * We simulate network delay by scheduling the delivery after a short random delay.
         */
        private void sendMessage(int peerId, RaftMessage message) {
            for (RaftNode peer : peers) {
                if (peer.id == peerId) {
                    // Simulate network delay of 50-150 ms.
                    scheduler.schedule(() -> peer.receiveMessage(message), random.nextInt(100) + 50, TimeUnit.MILLISECONDS);
                    break;
                }
            }
        }

        /**
         * Called by other nodes to deliver a message to this node.
         */
        public void receiveMessage(RaftMessage message) {
            messageQueue.offer(message);
        }

        /**
         * Start an election by becoming a candidate, incrementing our term, voting for self,
         * and sending RequestVote RPCs to all peers.
         */
        private void startElection() {
            role = Role.CANDIDATE;
            currentTerm++;
            votedFor = id; // Vote for self.
            voteCount = 1;   // Count our own vote.
            System.out.println("Node " + id + " starting election for term " + currentTerm);
            int lastLogIndex = log.size() - 1;
            int lastLogTerm = lastLogIndex >= 0 ? log.get(lastLogIndex).term : 0;
            RequestVote rv = new RequestVote(currentTerm, id, lastLogIndex, lastLogTerm);
            for (RaftNode peer : peers) {
                sendMessage(peer.id, rv);
            }
            // Reset election timeout in case this election fails.
            resetElectionTimeout();
        }

        /**
         * Shutdown this node by stopping its main loop and scheduler.
         */
        public void shutdown() {
            running = false;
            this.interrupt();
            scheduler.shutdownNow();
        }
    }

    // --- Main Method and Tests ---
    /**
     * The main method simulates the cluster. It creates 5 Raft nodes, sets their peers,
     * starts them, waits for leader election, submits some client commands to the leader,
     * and then prints out the state machines of all nodes.
     */
    public static void main(String[] args) throws Exception {
        // Create a cluster of 5 nodes.
        int numNodes = 5;
        List<RaftNode> cluster = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            RaftNode node = new RaftNode(i);
            cluster.add(node);
        }
        // Set each node’s peers (all other nodes in the cluster).
        for (RaftNode node : cluster) {
            List<RaftNode> peers = new ArrayList<>(cluster);
            peers.remove(node);
            node.setPeers(peers);
        }
        // Start all nodes.
        for (RaftNode node : cluster) {
            node.start();
        }

        // Wait a few seconds to allow leader election.
        Thread.sleep(5000);

        // Find the current leader.
        RaftNode leader = null;
        for (RaftNode node : cluster) {
            if (node.role == Role.LEADER) {
                leader = node;
                break;
            }
        }
        if (leader == null) {
            System.out.println("No leader elected. Exiting test.");
        } else {
            System.out.println("Leader elected: Node " + leader.id);

            // Submit several commands (simulating client 'put' requests) to the leader.
            leader.submitCommand("put key1 value1");
            leader.submitCommand("put key2 value2");
            leader.submitCommand("put key3 value3");
        }

        // Wait for log replication and command application.
        Thread.sleep(5000);

        // Print the state machine of every node.
        System.out.println("\n--- State Machines of All Nodes ---");
        for (RaftNode node : cluster) {
            System.out.println("Node " + node.id + " state machine: " + node.stateMachine);
        }

        // Shutdown all nodes.
        for (RaftNode node : cluster) {
            node.shutdown();
        }

        System.out.println("Test completed.");
    }
}