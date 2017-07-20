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

import org.neo4j.causalclustering.catchup.CatchUpResponseAdaptor;
import org.neo4j.causalclustering.catchup.TxPullRequestResult;
import org.neo4j.causalclustering.catchup.tx.TxPullResponse;
import org.neo4j.causalclustering.catchup.tx.TxPullResponseListener;
import org.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;

public class TxPullResponseAdaptor extends CatchUpResponseAdaptor<TxPullRequestResult>
{
    private long lastTxIdReceived;
    private final TxPullResponseListener txPullResponseListener;

    public TxPullResponseAdaptor( TxPullResponseListener txPullResponseListener, long lastTxIdReceived )
    {
        this.lastTxIdReceived = lastTxIdReceived;
        this.txPullResponseListener = txPullResponseListener;
    }

    @Override
    public void onTxPullResponse( CompletableFuture<TxPullRequestResult> signal, TxPullResponse response )
    {
        this.lastTxIdReceived = response.tx().getCommitEntry().getTxId();
        txPullResponseListener.onTxReceived( response );
    }

    @Override
    public void onTxStreamFinishedResponse( CompletableFuture<TxPullRequestResult> signal, TxStreamFinishedResponse response )
    {
        signal.complete( new TxPullRequestResult( response.status(), lastTxIdReceived ) );
    }
}
