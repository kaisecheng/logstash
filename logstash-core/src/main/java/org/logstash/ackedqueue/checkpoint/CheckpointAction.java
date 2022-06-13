package org.logstash.ackedqueue.checkpoint;

import org.logstash.ackedqueue.Checkpoint;
import org.logstash.ackedqueue.Page;
import org.logstash.ackedqueue.Queue;
import org.logstash.ackedqueue.io.CheckpointIO;

import java.io.IOException;

public abstract class CheckpointAction {
    protected final Page page;
    protected Checkpoint lastCp;
    protected final CheckpointIO cpIO;
    protected final int cpMaxAcks;
    protected final int cpMaxWrites;
    protected final Queue queue;

    protected CheckpointAction(Page page, int checkpointMaxAcks, int checkpointMaxWrites, Queue queue) {
        this.page = page;
        this.lastCp = new Checkpoint(0, 0, 0, 0, 0);
        this.cpIO = queue.getCheckpointIO();
        this.cpMaxAcks = checkpointMaxAcks;
        this.cpMaxWrites = checkpointMaxWrites;
        this.queue = queue;
    }

    public void writeCheckpointFile() throws IOException {
        Checkpoint checkpoint = getNewCheckpoint();
        cpIO.write(getFileName(), checkpoint);
        lastCp = checkpoint;
    }

    public void writeCheckpointFile(Checkpoint checkpoint) throws IOException {
        cpIO.write(getFileName(), checkpoint);
        lastCp = checkpoint;
    }

    public Checkpoint getNewCheckpoint() {
        return new Checkpoint(this.page.getPageNum(), getFirstUnackedPageNum(), this.page.firstUnackedSeqNum(), this.page.getMinSeqNum(), this.page.getElementCount());
    }

    public void checkpointAck(boolean isFullyAcked) throws IOException {
        if (isFullyAcked || (cpMaxAcks > 0 && this.page.firstUnackedSeqNum() >= lastCp.getFirstUnackedSeqNum() + cpMaxAcks)) {
            updateAckPage(isFullyAcked);
        }
    }

    public abstract void updateAckPage(boolean isFullyAcked) throws IOException;
    public abstract void checkpoint() throws IOException;
    public abstract String getFileName();
    public abstract int getFirstUnackedPageNum();
}
