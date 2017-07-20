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
package org.neo4j.nextgenbackup;

import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchUpClientException;
import org.neo4j.causalclustering.catchup.CatchUpResponseAdaptor;
import org.neo4j.causalclustering.catchup.HughCatchUpClient;
import org.neo4j.causalclustering.catchup.TxPullRequestResult;
import org.neo4j.causalclustering.catchup.tx.PullRequestMonitor;
import org.neo4j.causalclustering.catchup.tx.TxPullRequest;
import org.neo4j.causalclustering.catchup.tx.TxPullResponse;
import org.neo4j.causalclustering.catchup.tx.TxPullResponseListener;
import org.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.monitoring.Monitors;

public class HughTxPullClient
{
    private final HughCatchUpClient catchUpClient;
    private PullRequestMonitor pullRequestMonitor;

    public HughTxPullClient( HughCatchUpClient catchUpClient, Monitors monitors )
    {
        this.catchUpClient = catchUpClient;
        this.pullRequestMonitor = monitors.newMonitor( PullRequestMonitor.class );
    }

    public TxPullRequestResult pullTransactions( AdvertisedSocketAddress from, StoreId storeId, long previousTxId,
            TxPullResponseListener txPullResponseListener ) throws CatchUpClientException
    {
        pullRequestMonitor.txPullRequest( previousTxId );
        return catchUpClient.makeBlockingRequest( from, new TxPullRequest( previousTxId, storeId ),
                new TxPullResponseAdaptor( txPullResponseListener, previousTxId ) );
    }
}

