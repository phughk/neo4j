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

import org.junit.Before;
import org.junit.Test;

import org.neo4j.causalclustering.catchup.CatchUpClientException;
import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.HughCatchUpClient;
import org.neo4j.causalclustering.catchup.TxPullRequestResult;
import org.neo4j.causalclustering.catchup.tx.PullRequestMonitor;
import org.neo4j.causalclustering.catchup.tx.TxPullRequest;
import org.neo4j.causalclustering.catchup.tx.TxPullResponseListener;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.monitoring.Monitors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.verify;

public class HughTxPullClientTest
{
    AdvertisedSocketAddress fromAddress = new AdvertisedSocketAddress( "localhost", 1336 );
    StoreId storeId = new StoreId( 6, 2, 6, 7 );
    long previousTxId = 266;
    TxPullResponseListener txPullResponseListener = mock( TxPullResponseListener.class );
    PullRequestMonitor pullRequestMonitor = mock( PullRequestMonitor.class );

    HughCatchUpClient hughCatchUpClient;
    Monitors monitors;

    HughTxPullClient subject;

    @Before
    public void setup()
    {
        fromAddress = new AdvertisedSocketAddress( "localhost", 1336 );
        storeId = new StoreId( 6, 2, 6, 7 );
        previousTxId = 266;
        txPullResponseListener = mock( TxPullResponseListener.class );

        hughCatchUpClient = mock( HughCatchUpClient.class );
        monitors = mock( Monitors.class );
        stub( monitors.newMonitor( PullRequestMonitor.class ) ).toReturn( pullRequestMonitor );
        subject = new HughTxPullClient( hughCatchUpClient, monitors );
    }

    @Test
    public void requestForTransactionsDelegatesToCatchUpClient() throws CatchUpClientException
    {
        // given
        long lastTransactionId = 1634L;
        TxPullRequestResult expectedResult = new TxPullRequestResult( CatchupResult.SUCCESS_END_OF_STREAM, lastTransactionId );

        // and
        stub( hughCatchUpClient.makeBlockingRequest( any(), any(), any() ) ).toReturn( expectedResult );

        // when
        TxPullRequestResult result = subject.pullTransactions( fromAddress, storeId, previousTxId, txPullResponseListener );

        // then
        verify( hughCatchUpClient ).makeBlockingRequest( eq( fromAddress ), any( TxPullRequest.class ), any( TxPullResponseAdaptor.class ) );
        assertEquals( expectedResult, result );
    }

    @Test
    public void pullRequestMonitorIsNotifiedOfPreviousTransactionAdState() throws CatchUpClientException
    {
        // when
        subject.pullTransactions( fromAddress, storeId, previousTxId, txPullResponseListener );

        // then
        verify( pullRequestMonitor ).txPullRequest( previousTxId );
    }
}
