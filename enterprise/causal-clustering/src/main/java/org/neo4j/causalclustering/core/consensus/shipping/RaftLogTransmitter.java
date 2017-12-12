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

import io.netty.util.concurrent.Future;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.function.Consumer;

import org.neo4j.causalclustering.core.consensus.LeaderContext;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.messaging.Outbound;
import org.neo4j.logging.Log;

import static java.lang.Long.max;
import static java.lang.Long.min;
import static java.lang.String.format;
import static org.neo4j.causalclustering.core.consensus.schedule.RenewableTimeoutService.RenewableTimeout;
import static org.neo4j.causalclustering.core.consensus.shipping.RaftLogShipper.Mode.CATCHUP;
import static org.neo4j.causalclustering.core.consensus.shipping.RaftLogShipper.Mode.PIPELINE;
import static org.neo4j.causalclustering.core.consensus.shipping.RaftLogShipper.Timeouts.RESEND;

public class RaftLogTransmitter
{
    private final Outbound<MemberId,RaftMessages.RaftMessage> outbound;

    public RaftLogTransmitter( Outbound<MemberId,RaftMessages.RaftMessage> outbound )
    {
        this.outbound = outbound;
    }

    public void sendEmptyAppendWithMetaData( MemberId follower, MemberId leader, LeaderContext leaderContext, long prevLogIndex,
            long prevLogTerm )
    {
        RaftMessages.AppendEntries.Request appendRequest =
                new RaftMessages.AppendEntries.Request( leader, leaderContext.term, prevLogIndex, prevLogTerm, RaftLogEntry.empty, leaderContext.commitIndex );
        outbound.send( follower, appendRequest );
    }

    public void sendCommitUpdate( MemberId follower, MemberId leader, LeaderContext leaderContext )
    {
        /*
         * This is a commit update. That means that we just received enough success responses to an append
         * request to allow us to send a commit. By Raft invariants, this means that the term for the committed
         * entry is the current term.
         */
        RaftMessages.Heartbeat appendRequest = new RaftMessages.Heartbeat( leader, leaderContext.term, leaderContext.commitIndex, leaderContext.term );

        outbound.send( follower, appendRequest );
    }

    public void sendLogCompactionInfo( MemberId follower, MemberId leader, LeaderContext leaderContext, long prevLogIndex )
    {
        outbound.send( follower, new RaftMessages.LogCompactionInfo( leader, leaderContext.term, prevLogIndex ) );
    }

    public void sendRange( MemberId leader, LeaderContext leaderContext, long prevLogIndex, long prevLogTerm, InstanceInfo instanceInfo,
            MemberId follower, Log log, Runnable scheduleTimeout, RaftLogEntry[] entries )
    {

        RaftMessages.AppendEntries.Request appendRequest =
                new RaftMessages.AppendEntries.Request( leader, leaderContext.term, prevLogIndex, prevLogTerm, entries, leaderContext.commitIndex );
        instanceInfo.sendToFollower( appendRequest, LocalDateTime.now() );
        Future<Void> thefuture = outbound.send( follower, appendRequest );
        thefuture.addListener( future ->
        {
            log.info( "Sent complete at: " + LocalDateTime.now() );
//            scheduleTimeout( timeoutAbsoluteMillis );
            scheduleTimeout.run(); // TODO doesnt need to be a consumer as both params are received
        } );
    }

    public void sendNewEntries( MemberId leader, LeaderContext leaderContext, long prevLogIndex, long prevLogTerm,
            RaftLogEntry[] newEntries, MemberId follower )
    {
        RaftMessages.AppendEntries.Request appendRequest =
                new RaftMessages.AppendEntries.Request( leader, leaderContext.term, prevLogIndex, prevLogTerm, newEntries, leaderContext.commitIndex );

        outbound.send( follower, appendRequest );
    }
}
