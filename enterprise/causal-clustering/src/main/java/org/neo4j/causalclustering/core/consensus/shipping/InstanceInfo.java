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

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

import org.neo4j.causalclustering.core.consensus.RaftMessages;

public class InstanceInfo
{
    private final String id;
    private LocalDateTime start;
    private LocalDateTime startLogEntry;
    private LocalDateTime endLogEntry;
    private LocalDateTime end;
    private String sendInfo;
    private String entrtiesInfo;

    InstanceInfo( String s )
    {
        id = s;
    }

    public LocalDateTime getStart()
    {
        return start;
    }

    public LocalDateTime getStartLogEntry()
    {
        return startLogEntry;
    }

    /*alt insert -> getter/setter*/

    public String id()
    {
        return id;
    }

    public void start( LocalDateTime now )
    {
        start = now;
    }

    public LocalDateTime getEnd()
    {
        return end;
    }

    public long getEntryDuration()
    {
        return startLogEntry.until( endLogEntry, ChronoUnit.MILLIS );
    }

    public long getTotalDuration()
    {
        return start.until( end, ChronoUnit.MILLIS );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        InstanceInfo that = (InstanceInfo) o;
        return Objects.equals( id, that.id );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( id );
    }

    public void startLogEnry( LocalDateTime now )
    {
        startLogEntry = now;
    }

    public void endLogEntry( LocalDateTime now )
    {
        endLogEntry = now;
    }

    public void end( LocalDateTime now )
    {
        end = now;
    }

    public void sendToFollower( RaftMessages.AppendEntries.Request appendRequest, LocalDateTime now )
    {
        sendInfo = String.format( "Sent %s at %s", appendRequest, now );
    }

    public void entreisInfo( int length, long sum )
    {
        entrtiesInfo = String.format( "Total entries: %s. With total size %s (bytes)", length, sum );
    }

    public String sendInfo()
    {
        return sendInfo;
    }

    public String entrtiesInfo()
    {
        return entrtiesInfo;
    }
}
