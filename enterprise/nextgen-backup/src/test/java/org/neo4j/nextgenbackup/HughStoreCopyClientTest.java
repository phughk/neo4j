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
import org.mockito.Mockito;

import org.neo4j.causalclustering.catchup.CatchUpClientException;
import org.neo4j.causalclustering.catchup.HughCatchUpClient;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdRequest;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreRequest;
import org.neo4j.causalclustering.catchup.storecopy.StoreFileStreams;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.nextgenbackup.TestHelpers.executionIsExpectedToFail;

public class HughStoreCopyClientTest
{
    HughCatchUpClient hughCatchUpClient = Mockito.mock( HughCatchUpClient.class );
    LogProvider logProvider = NullLogProvider.getInstance();
    AdvertisedSocketAddress fromAddress = new AdvertisedSocketAddress( "localhost", 1234 );
    StoreId expectedStoreId;
    StoreFileStreams storeFileStreams;

    HughStoreCopyClient subject;

    @Before
    public void setup()
    {
        subject = new HughStoreCopyClient( hughCatchUpClient, logProvider );
    }

    @Test
    public void lastTransactionIsReturnedWhenCopyingStoreFiles() throws CatchUpClientException
    {
        // given
        stub( hughCatchUpClient.makeBlockingRequest( eq( fromAddress ), any(), any( CopyStoreFilesResponseAdaptor.class ) ) ).toReturn( 123L );

        // when
        long latestTransactionId = subject.copyStoreFiles( fromAddress, expectedStoreId, storeFileStreams );

        // then
        verify( hughCatchUpClient ).makeBlockingRequest( eq( fromAddress ), any( GetStoreRequest.class ), any( CopyStoreFilesResponseAdaptor.class ) );
        assertEquals( 123L, latestTransactionId );
    }

    @Test
    public void copyStoreExceptionsAreWrappedInStoreCopyFailedException() throws CatchUpClientException
    {
        // given
        when( hughCatchUpClient.makeBlockingRequest( any(), any(), any( CopyStoreFilesResponseAdaptor.class ) ) ).thenThrow( CatchUpClientException.class );

        // when
        StoreCopyFailedException exception =
                executionIsExpectedToFail( () -> subject.copyStoreFiles( fromAddress, expectedStoreId, storeFileStreams ), StoreCopyFailedException.class );

        // then
        assertEquals( CatchUpClientException.class, exception.getCause().getClass() );
    }

    @Test
    public void storeIdIsRetrievedWithTheCorrectRequestAndResponseAdapter() throws CatchUpClientException
    {
        // when
        subject.fetchStoreId( fromAddress );

        // then
        verify( hughCatchUpClient ).makeBlockingRequest( eq( fromAddress ), any( GetStoreIdRequest.class ), any( GetStoreIdResponseAdaptor.class ) );
    }

    @Test
    public void blockingRequestExceptionsAreWrappedInSpecificException() throws CatchUpClientException
    {
        // given
        when( hughCatchUpClient.makeBlockingRequest( any(), any(), any() ) ).thenThrow( CatchUpClientException.class );

        // when
        StoreIdDownloadFailedException exception = executionIsExpectedToFail( () -> subject.fetchStoreId( fromAddress ), StoreIdDownloadFailedException.class );

        // then
        assertEquals( CatchUpClientException.class, exception.getCause().getClass() );
    }
}
