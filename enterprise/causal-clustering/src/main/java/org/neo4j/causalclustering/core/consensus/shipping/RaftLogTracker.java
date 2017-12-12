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

import java.util.stream.Stream;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class RaftLogTracker
{
    private static final long MIN_INDEX = 1L;

    private long lastSentIndex;
    private long matchIndex = -1;
    private final int maxAllowedShippingLag;
    private final int catchupBatchSize;

    public RaftLogTracker( int maxAllowedShippingLag, int catchupBatchSize )
    {
        this.maxAllowedShippingLag = maxAllowedShippingLag;
        this.catchupBatchSize = catchupBatchSize;
    }

    public long getLogIndex( long lastRemoteAppendIndex )
    {
        return max( min( lastSentIndex - 1, lastRemoteAppendIndex ), MIN_INDEX );
    }

    public long getLastSentIndex()
    {
        return lastSentIndex;
    }

    /**
     * Replaces `set` as it also returns if there was a change
     * @param newMatchIndex
     * @return
     */
    public boolean updateMatchIndex( long newMatchIndex )
    {
        if ( newMatchIndex > matchIndex )
        {
            System.out.format( "Updated match index %d with %d\n", matchIndex, newMatchIndex );
            matchIndex = newMatchIndex;
            return true;
        }
        System.out.printf( "Not updated match index %d with %d\n", matchIndex, newMatchIndex );
        return false;
    }

    public boolean lastIndexHasCaughtUp()
    {
        return matchIndex >= lastSentIndex;
    }

    public boolean lastSentIndexIsTheOneWeAreMatchingAgainst()
    {
        return matchIndex == lastSentIndex;
    }

    public void setLastSentIndex( long lastSentIndex )
    {
        this.lastSentIndex = lastSentIndex;
    }

    public boolean lastSentIndexStillNotCaughtUp( long prevLogIndex )
    {
        return lastSentIndex <= prevLogIndex;
    }

    public boolean isShippingLag( long prevLogIndex )
    {
        return prevLogIndex - matchIndex <= maxAllowedShippingLag;
    }

    public int getMaxAllowedShippingLag()
    {
        return maxAllowedShippingLag;
    }

    public boolean lastIndexGreaterThanMatching( long lastIndex )
    {
        return lastIndex > matchIndex;
    }

    public long endIndexFromBatch( long lastIndex )
    {
        return min( lastIndex, matchIndex + catchupBatchSize );
    }

    public long getMatchIndex()
    {
        return matchIndex;
    }
}
