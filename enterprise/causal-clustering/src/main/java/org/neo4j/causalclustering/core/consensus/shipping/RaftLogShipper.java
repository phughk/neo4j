/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.core.consensus.shipping;

import java.io.IOException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Arrays;

import org.neo4j.causalclustering.core.consensus.LeaderContext;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.neo4j.causalclustering.core.consensus.log.monitoring.RaftLogShipperMonitoring;
import org.neo4j.causalclustering.core.consensus.schedule.DelayedRenewableTimeoutService;
import org.neo4j.causalclustering.core.consensus.schedule.RenewableTimeoutService;
import org.neo4j.causalclustering.core.state.InFlightLogEntryReader;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.Outbound;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.Long.max;
import static java.lang.String.format;
import static org.neo4j.causalclustering.core.consensus.schedule.RenewableTimeoutService.RenewableTimeout;
import static org.neo4j.causalclustering.core.consensus.shipping.RaftLogShipper.Mode.CATCHUP;
import static org.neo4j.causalclustering.core.consensus.shipping.RaftLogShipper.Mode.PIPELINE;
import static org.neo4j.causalclustering.core.consensus.shipping.RaftLogShipper.Timeouts.RESEND;

/// Optimizations
// TODO: Have several outstanding batches in catchup mode, to bridge the latency gap.
// TODO: Bisect search for mismatch.
// TODO: Maximum bound on size of batch in bytes, not just entry count.

// Production ready
// TODO: Replace sender service with something more appropriate. No need for queue and multiplex capability, in fact
// is it bad to have?
//  TODO Should we drop messages to unconnected channels instead? Use UDP? Because we are not allowed to go below a
// certain cluster size (safety)
//  TODO then leader will keep trying to replicate to gone members, thus queuing things up is hurtful.

// TODO: Replace the timeout service with something better. More efficient for the constantly rescheduling use case
// and also useful for deterministic unit tests.

// Core functionality
// TODO: Consider making even CommitUpdate a raft-message of its own.

/**
 * This class handles the shipping of raft logs from this node when it is the leader to the followers.
 * Each instance handles a single follower and acts on events and associated state updates originating
 * within the main raft state machine.
 * <p>
 * It is crucial that all actions happen within the context of the leaders state at some point in time.
 */
public class RaftLogShipper
{
    // we never ship entry zero, which must be bootstrapped or received as part of a snapshot
    private final int TIMER_INACTIVE = 0;
    private static final long MAX_BATCH_BYTE_SIZE = 1_073_741_824;

    enum Mode
    {
        /**
         * In the mismatch mode we are unsure about the follower state, thus
         * we tread with caution, going backwards trying to find the point where
         * our logs match. We send empty append entries to minimize the cost of
         * this mode.
         */
        MISMATCH,
        /**
         * In the catchup mode we are trying to catch up the follower as quickly
         * as possible. The follower receives batches of entries in series until
         * it is fully caught up.
         */
        CATCHUP,
        /**
         * In the pipeline mode the follower is treated as caught up and we
         * optimistically ship any latest entries without waiting for responses,
         * expecting successful responses.
         */
        PIPELINE
    }

    public enum Timeouts implements RenewableTimeoutService.TimeoutName
    {
        RESEND
    }

    private final LogProvider logProvider;
    private final Log log;
    private final ReadableRaftLog raftLog;
    private final Clock clock;
    private final MemberId follower;
    private final MemberId leader;
    private final long retryTimeMillis;
    private final RaftLogShipperMonitoring raftLogShipperMonitoring;
    private final InFlightCache inFlightCache;
    private final RaftLogTransmitter raftLogTransmitter;
    private final RaftLogTracker raftLogTracker;

    private DelayedRenewableTimeoutService timeoutService;
    private RenewableTimeout timeout;

    // state
    private long timeoutAbsoluteMillis;
    private LeaderContext lastLeaderContext;
    private Mode mode = Mode.MISMATCH;

    RaftLogShipper( Outbound<MemberId,RaftMessages.RaftMessage> outbound, LogProvider logProvider,
            ReadableRaftLog raftLog, Clock clock,
            MemberId leader, MemberId follower, long leaderTerm, long leaderCommit, long retryTimeMillis,
            int catchupBatchSize, int maxAllowedShippingLag, InFlightCache inFlightCache,
            RaftLogShipperMonitoring raftLogShipperMonitoring )
    {
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
        this.raftLog = raftLog;
        this.clock = clock;
        this.follower = follower;
        this.leader = leader;
        this.retryTimeMillis = retryTimeMillis;
        this.raftLogShipperMonitoring = raftLogShipperMonitoring;
        this.lastLeaderContext = new LeaderContext( leaderTerm, leaderCommit );
        this.inFlightCache = inFlightCache;
        this.raftLogTransmitter = new RaftLogTransmitter( outbound );
        this.raftLogTracker = new RaftLogTracker( maxAllowedShippingLag, catchupBatchSize );
    }

    public Object identity()
    {
        return follower;
    }

    public synchronized void start()
    {
        log.info( "Starting log shipper: %s", statusAsString() );
        timeoutService = new DelayedRenewableTimeoutService( clock, logProvider );
        timeoutService.init();
        timeoutService.start();
        sendEmptyAppendWithMetaData( raftLog.appendIndex(), lastLeaderContext );
    }

    public synchronized void stop()
    {
        log.info( "Stopping log shipper %s", statusAsString() );

        try
        {
            timeoutService.stop();
            timeoutService.shutdown();
        }
        catch ( Throwable e )
        {
            log.error( "Failed to stop log shipper " + statusAsString(), e );
        }
        abortTimeout();
    }

    public synchronized void onMismatch( long lastRemoteAppendIndex, LeaderContext leaderContext )
    {
        switch ( mode )
        {
        case MISMATCH:
            long logIndex = raftLogTracker.getLogIndex( lastRemoteAppendIndex );
            sendEmptyAppendWithMetaData( logIndex, leaderContext );
            break;
        case PIPELINE:
        case CATCHUP:
            log.info( "%s: mismatch in mode %s from follower %s, moving to MISMATCH mode",
                    statusAsString(), mode, follower );
            mode = Mode.MISMATCH;
            sendEmptyAppendWithMetaData( raftLogTracker.getLastSentIndex(), leaderContext );
            break;

        default:
            throw new IllegalStateException( "Unknown mode: " + mode );
        }

        lastLeaderContext = leaderContext;
    }

    public synchronized void onMatch( long newMatchIndex, LeaderContext leaderContext )
    {
        boolean progress = raftLogTracker.updateMatchIndex( newMatchIndex );
        if ( !progress )
        {
            log.warn( "%s: match index not progressing. This should be transient.", statusAsString() );
        }

        switch ( mode )
        {
        case MISMATCH:
            establishWhichStateIsCorrectSinceNoLongerInCorrectState( leaderContext );
            break;
        case CATCHUP:
            establishIfStoppedCatchingUp( leaderContext );
            break;
        case PIPELINE:
            measureEffectivenessOfPassiveCatchup( progress );
            break;
        default:
            throw new IllegalStateException( "Unknown mode: " + mode );
        }

        lastLeaderContext = leaderContext;
    }

    private void measureEffectivenessOfPassiveCatchup( boolean progress )
    {
        if ( raftLogTracker.lastSentIndexIsTheOneWeAreMatchingAgainst() )
        {
            abortTimeout();
        }
        else if ( progress )
        {
            scheduleTimeout( retryTimeMillis );
        }
    }

    private void establishIfStoppedCatchingUp( LeaderContext leaderContext )
    {
        if ( raftLogTracker.lastIndexHasCaughtUp() )
        {
            if ( sendNextBatchAfterMatch( leaderContext ) )
            {
                log.info( "%s: caught up, moving to PIPELINE mode", statusAsString() );
                mode = PIPELINE;
            }
        }
    }

    private void establishWhichStateIsCorrectSinceNoLongerInCorrectState( LeaderContext leaderContext )
    {
        if ( sendNextBatchAfterMatch( leaderContext ) )
        {
            log.info( "%s: caught up after mismatch, moving to PIPELINE mode", statusAsString() );
            mode = PIPELINE;
        }
        else
        {
            log.info( "%s: starting catch up after mismatch, moving to CATCHUP mode", statusAsString() );
            mode = Mode.CATCHUP;
        }
    }

    public synchronized void onNewEntries( long prevLogIndex, long prevLogTerm, RaftLogEntry[] newLogEntries,
            LeaderContext leaderContext )
    {
        if ( mode == Mode.PIPELINE )
        {
            updateEverythingThatNeedsUpdating( prevLogIndex, prevLogTerm, newLogEntries, leaderContext );
        }

        lastLeaderContext = leaderContext;
    }

    private void updateEverythingThatNeedsUpdating( long prevLogIndex, long prevLogTerm, RaftLogEntry[] newLogEntries, LeaderContext leaderContext )
    {
        while ( raftLogTracker.lastSentIndexStillNotCaughtUp( prevLogIndex ) )
        {
            if ( raftLogTracker.isShippingLag( prevLogIndex ) )
            {
                // all sending functions update lastSentIndex
                sendNewEntries( prevLogIndex, prevLogTerm, newLogEntries, leaderContext );
            }
            else
            {
                    /* The timer is still set at this point. Either we will send the next batch
                     * as soon as the follower has caught up with the last pipelined entry,
                     * or when we timeout and resend. */
                log.info( "%s: follower has fallen behind (target prevLogIndex was %d, maxAllowedShippingLag " +
                                "is %d), moving to CATCHUP mode", statusAsString(), prevLogIndex,
                        raftLogTracker.getMaxAllowedShippingLag() );
                mode = Mode.CATCHUP;
                break;
            }
        }
    }

    public synchronized void onCommitUpdate( LeaderContext leaderContext )
    {
        if ( mode == Mode.PIPELINE )
        {
            raftLogTransmitter.sendCommitUpdate( follower, leader, leaderContext );
        }

        lastLeaderContext = leaderContext;
    }

    /**
     * Callback invoked by the external timer service.
     */
    private synchronized void onScheduledTimeoutExpiry()
    {
        if ( timedOut() )
        {
            onTimeout();
        }
        else if ( timeoutAbsoluteMillis != TIMER_INACTIVE )
        {
            /* Timer was moved, so we need to reschedule. */
            long timeLeft = timeoutAbsoluteMillis - clock.millis();
            if ( timeLeft > 0 )
            {
                scheduleTimeout( timeLeft );
            }
            else
            {
                /* However it managed to expire, so we can just handle it immediately. */
                onTimeout();
            }
        }
    }

    void onTimeout()
    {
        if ( mode == PIPELINE )
        {
            /* The follower seems unresponsive and we do not want to spam it with new entries.
             * The catchup will pick-up when the last sent pipelined entry matches. */
            log.info( "%s: timed out, moving to CATCHUP mode", statusAsString() );
            mode = Mode.CATCHUP;
            scheduleTimeout( retryTimeMillis );
        }
        else if ( mode == CATCHUP )
        {
            /* The follower seems unresponsive so we move back to mismatch mode to
             * slowly poke it and figure out what is going on. Catchup will resume
             * on the next match. */
            log.info( "%s: timed out, moving to MISMATCH mode", statusAsString() );
            mode = Mode.MISMATCH;
        }

        if ( lastLeaderContext != null )
        {
            sendEmptyAppendWithMetaData( raftLogTracker.getLastSentIndex(), lastLeaderContext );
        }
    }

    /**
     * This function is necessary because the scheduled callback blocks on the monitor before
     * entry and the expiry time of the timer might have been moved or even cancelled before
     * the entry is granted.
     *
     * @return True if we actually timed out, otherwise false.
     */
    private boolean timedOut()
    {
        return timeoutAbsoluteMillis != TIMER_INACTIVE && (clock.millis() - timeoutAbsoluteMillis) >= 0;
    }

    private void scheduleTimeout( long deltaMillis )
    {
        // TODO: This cancel/create dance is a bit inefficient... consider something better.

        timeoutAbsoluteMillis = clock.millis() + deltaMillis;

        if ( timeout != null )
        {
            timeout.cancel();
        }
        timeout = timeoutService.create( RESEND, deltaMillis, 0, timeout -> onScheduledTimeoutExpiry() );
    }

    private void abortTimeout()
    {
        if ( timeout != null )
        {
            timeout.cancel();
        }
        timeoutAbsoluteMillis = TIMER_INACTIVE;
    }

    /**
     * Returns true if this sent the last batch.
     */
    private boolean sendNextBatchAfterMatch( LeaderContext leaderContext )
    {
        InstanceInfo instanceInfo = new InstanceInfo( followerId() );
        instanceInfo.start( LocalDateTime.now() );
        long lastIndex = raftLog.appendIndex();

        if ( raftLogTracker.lastIndexGreaterThanMatching( lastIndex ) )
        {
            long endIndex = raftLogTracker.endIndexFromBatch( lastIndex );

//            scheduleTimeout( retryTimeMillis );
            sendRange( raftLogTracker.getMatchIndex() + 1, endIndex, leaderContext, instanceInfo );
            instanceInfo.end( LocalDateTime.now() );
            raftLogShipperMonitoring.register( instanceInfo );
            return endIndex == lastIndex;
        }
        else
        {
            instanceInfo.end( LocalDateTime.now() );
            raftLogShipperMonitoring.register( instanceInfo );
            return true;
        }
    }

    private String followerId()
    {
        return follower.getUuid().toString();
    }

    private void sendNewEntries( long prevLogIndex, long prevLogTerm, RaftLogEntry[] newEntries,
            LeaderContext leaderContext )
    {
        scheduleTimeout( retryTimeMillis );

        raftLogTracker.setLastSentIndex( prevLogIndex + 1 );
        raftLogTransmitter.sendNewEntries( leader, leaderContext, prevLogIndex, prevLogTerm, newEntries, follower );
    }

    /**
     * Send an empty append entry that also contains meta data about the raft state.
     * @param logIndex what the log points to
     * @param leaderContext who the leader is
     */
    private void sendEmptyAppendWithMetaData( long logIndex, LeaderContext leaderContext )
    {
        scheduleTimeout( retryTimeMillis );

        logIndex = max( raftLog.prevIndex() + 1, logIndex );
        raftLogTracker.setLastSentIndex( logIndex );

        try
        {
            long prevLogIndex = logIndex - 1;
            long prevLogTerm = raftLog.readEntryTerm( prevLogIndex );

            if ( prevLogTerm > leaderContext.term )
            {
                log.warn( "%s: aborting send. Not leader anymore? %s, prevLogTerm=%d",
                        statusAsString(), leaderContext, prevLogTerm );
                return;
            }

            if ( doesNotExistInLog( prevLogIndex, prevLogTerm ) )
            {
                log.warn( "%s: Entry was pruned when sending empty (prevLogIndex=%d, prevLogTerm=%d)",
                        statusAsString(), prevLogIndex, prevLogTerm );
                return;
            }

            raftLogTransmitter.sendEmptyAppendWithMetaData( follower, leader, leaderContext, prevLogIndex, prevLogTerm );
        }
        catch ( IOException e )
        {
            log.warn( statusAsString() + " exception during empty send", e );
        }
    }

    private void sendRange( long startIndex, long endIndex, LeaderContext leaderContext, InstanceInfo instanceInfo )
    {
        if ( startIndex > endIndex )
        {
            return;
        }

        raftLogTracker.setLastSentIndex( endIndex );

        try
        {
            int batchSize = (int) (endIndex - startIndex + 1);
            RaftLogEntry[] entries = new RaftLogEntry[batchSize];

            long prevLogIndex = startIndex - 1;
            long prevLogTerm = raftLog.readEntryTerm( prevLogIndex );

            if ( prevLogTerm > leaderContext.term )
            {
                log.warn( "%s aborting send. Not leader anymore? %s, prevLogTerm=%d",
                        statusAsString(), leaderContext, prevLogTerm );
                return;
            }

            instanceInfo.startLogEnry( LocalDateTime.now() );
            int offset = 0;
            boolean entryMissing = false;
            osnidf();
            instanceInfo.endLogEntry( LocalDateTime.now() );

            if ( entryMissing || doesNotExistInLog( prevLogIndex, prevLogTerm ) )
            {
                if ( raftLog.prevIndex() >= prevLogIndex )
                {
                    sendLogCompactionInfo( leaderContext );
                }
                else
                {
                    log.error( "%s: Could not send compaction info and entries were missing, but log is not behind.",
                            statusAsString() );
                }
            }
            else
            {
                if ( offset < batchSize )
                {
                    entries = Arrays.copyOf( entries, offset );
                }
                long sum = Arrays.stream( entries ).mapToLong(
                        value ->
                        {
                            if ( value.content().hasSize() )
                            {
                                return value.content().size();
                            }
                            return 0;
                        }
                ).sum();
                instanceInfo.entreisInfo( entries.length, sum );

                raftLogTransmitter.sendRange( leader, leaderContext, prevLogIndex, prevLogTerm, instanceInfo, follower, log,
                        () -> scheduleTimeout( timeoutAbsoluteMillis ), entries );
            }
        }
        catch ( IOException e )
        {
            log.warn( statusAsString() + " exception during batch send", e );
        }
    }

    private Result osnidf(int batchSize, int startIndex, RaftLogEntry[] entries, LeaderContext leaderContext ) throws IOException
    {
        int offset = 0;
        try ( InFlightLogEntryReader logEntrySupplier = new InFlightLogEntryReader( raftLog, inFlightCache, false ) )
        {
            long aggregatedSize = 0;
            for ( ; offset < batchSize; offset++ )
            {
                RaftLogEntry raftLogEntry = logEntrySupplier.get( startIndex + offset );
                if ( raftLogEntry == null )
                {
                    return new Result( offset, true );
                }
                if ( raftLogEntry.term() > leaderContext.term )
                {
                    log.warn( "%s aborting send. Not leader anymore? %s, entryTerm=%d", statusAsString(), leaderContext, raftLogEntry.term() );
                    return new Result( offset, false );
                }
                if ( raftLogEntry.content().hasSize() )
                {
                    aggregatedSize += raftLogEntry.content().size();
                    if ( offset != 0 && aggregatedSize > MAX_BATCH_BYTE_SIZE )
                    {
                        offset--;
                        break;
                    }
                }
                entries[offset] = raftLogEntry;
            }
        }
        return new Result( offset, false );
    }

    private static class Result
    {
        public final boolean entryMissing;
        public final int offset;

        public Result( int offset, boolean entryMissing )
        {
            this.offset = offset;
            this.entryMissing = entryMissing;
        }
    }

    private boolean doesNotExistInLog( long logIndex, long logTerm )
    {
        return logTerm == -1 && logIndex != -1;
    }

    private void sendLogCompactionInfo( LeaderContext leaderContext )
    {
        log.warn( "Sending log compaction info. Log pruned? Status=%s, LeaderContext=%s",
                statusAsString(), leaderContext );

        raftLogTransmitter.sendLogCompactionInfo( follower, leader, leaderContext, raftLog.prevIndex()  );
    }

    private String statusAsString()
    {
        return format( "%s[matchIndex: %d, lastSentIndex: %d, localAppendIndex: %d, mode: %s]", follower, raftLogTracker.getMatchIndex(),
                raftLogTracker.getLastSentIndex(), raftLog.appendIndex(), mode );
    }
}
