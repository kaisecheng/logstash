package org.logstash.ackedqueue.checkpoint;

import org.logstash.ackedqueue.Checkpoint;
import org.logstash.ackedqueue.Page;
import org.logstash.ackedqueue.Queue;

import java.io.IOException;

public class HeadCheckpointAction extends CheckpointAction {
    public HeadCheckpointAction(Page page, int checkpointMaxAcks, int checkpointMaxWrites, Queue queue){
        super(page, checkpointMaxAcks, checkpointMaxWrites, queue);
    }

    @Override
    public void checkpoint() throws IOException {
        if (page.getElementCount() > lastCp.getElementCount()) {
            // fsync & checkpoint if data written since last checkpoint
            page.getPageIO().ensurePersisted();
            writeCheckpointFile();
        } else {
            Checkpoint checkpoint = getNewCheckpoint();
            if (!checkpoint.equals(lastCp)) {
                // checkpoint only if it changed since last checkpoint
                // non-dry code with forceCheckpoint() to avoid unnecessary extra new Checkpoint object creation
                writeCheckpointFile(checkpoint);
            }
        }
    }

    @Override
    public void updateAckPage(boolean isFullyAcked) throws IOException {
        checkpoint();
    }

    // force a checkpoint if we wrote checkpointMaxWrites elements since last checkpoint
    // the initial condition of an "empty" checkpoint, maxSeqNum() will return -1
    public void checkpointWrite(long seqNum) throws IOException {
        if (cpMaxWrites > 0 && (seqNum >= this.lastCp.maxSeqNum() + cpMaxWrites)) {
            // did we write more than checkpointMaxWrites elements? if so checkpoint now
            checkpoint();
        }
    }

    public void ensurePersistedUpto(long seqNum) throws IOException {
        long lastCheckpointUptoSeqNum =  lastCp.getMinSeqNum() + lastCp.getElementCount();

        // if the last checkpoint for this headpage already included the given seqNum, no need to fsync/checkpoint
        if (seqNum > lastCheckpointUptoSeqNum) {
            // head page checkpoint does a data file fsync
            checkpoint();
        }
    }

    public TailCheckpointAction toTailCheckpointAction() throws IOException {
        return new TailCheckpointAction(page, cpMaxAcks, cpMaxWrites, queue);
    }

    @Override
    public String getFileName() {
        return cpIO.headFileName();
    }

    @Override
    public int getFirstUnackedPageNum() {
        return queue.firstUnackedPageNum();
    }
}
