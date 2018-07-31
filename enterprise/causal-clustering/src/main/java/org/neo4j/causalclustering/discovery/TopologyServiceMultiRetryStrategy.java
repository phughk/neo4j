/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.discovery;

import java.util.Optional;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.LogProvider;

public class TopologyServiceMultiRetryStrategy extends MultiRetryStrategy<MemberId,Optional<AdvertisedSocketAddress>> implements TopologyServiceRetryStrategy
{
    private long delayInMillis;
    private long retries;

    public TopologyServiceMultiRetryStrategy( long delayInMillis, long retries, LogProvider logProvider )
    {
        super( delayInMillis, retries, logProvider, TopologyServiceMultiRetryStrategy::sleep );
        this.delayInMillis = delayInMillis;
        this.retries = retries;
    }

    private static void sleep( long durationInMillis )
    {
        try
        {
            Thread.sleep( durationInMillis );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public String toString()
    {
        return String.format( "%s(delay=%d, retries=%d)", this.getClass().getName(), delayInMillis, retries );
    }
}
