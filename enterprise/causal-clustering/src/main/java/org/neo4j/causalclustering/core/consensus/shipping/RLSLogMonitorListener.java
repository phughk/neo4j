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

import org.neo4j.causalclustering.core.consensus.log.monitoring.RaftLogShipperMonitoring;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RLSLogMonitorListener implements RaftLogShipperMonitoring
{
    private final Log rlsLogger;

    public RLSLogMonitorListener( LogProvider logProvider )
    {
        rlsLogger = logProvider.getLog( RLSLogMonitorListener.class );
    }

    @Override
    public void register( InstanceInfo instanceInfo )
    {
        rlsLogger.info( String.format( "Log for: %s", instanceInfo.id() ) );
        rlsLogger.info( String.format( "End of batch to %s started %s ended %s lasted %s\n", instanceInfo.id(),
                instanceInfo
                        .getStart(), instanceInfo.getEnd(), instanceInfo.getTotalDuration()
        ) );
        rlsLogger.info( String.format( "Duration of LogEntry %s and Sent %s\n", instanceInfo.getEntryDuration(),
                instanceInfo.sendInfo() ) );
        rlsLogger.info( instanceInfo.entrtiesInfo() );
        rlsLogger.info( String.format( "End info for: %s", instanceInfo.id() ) );
    }
}
