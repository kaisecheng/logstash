package org.logstash.ackedqueue.checkpoint;

import org.logstash.ackedqueue.Page;
import org.logstash.ackedqueue.Queue;

import java.io.IOException;

public class TailCheckpointAction extends CheckpointAction {

    public TailCheckpointAction(Page page, int checkpointMaxAcks, int checkpointMaxWrites, Queue queue){
        super(page, checkpointMaxAcks, checkpointMaxWrites, queue);
    }

    @Override
    public void checkpoint() throws IOException {
        writeCheckpointFile();
    }

    @Override
    public void updateAckPage(boolean isFullyAcked) throws IOException {
        checkpoint();

        // purge fully acked tail page
        if (isFullyAcked) {
            page.purge();
            cpIO.purge(getFileName());
            page.assertFirstUnackedSeqNum();
        }
    }

    @Override
    public String getFileName() {
        return cpIO.tailFileName(page.getPageNum());
    }

    @Override
    public int getFirstUnackedPageNum() {
        return 0;
    }
}