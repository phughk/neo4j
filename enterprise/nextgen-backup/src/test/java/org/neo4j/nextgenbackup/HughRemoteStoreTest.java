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

import java.io.File;
import java.io.IOException;
import java.io.Writer;

import org.neo4j.causalclustering.catchup.TxPullRequestResult;
import org.neo4j.causalclustering.catchup.storecopy.StoreFileStreams;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpWriter;
import org.neo4j.causalclustering.catchup.tx.TxPullResponseListener;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static org.neo4j.nextgenbackup.TestHelpers.executionIsExpectedToFail;

public class HughRemoteStoreTest
{
    Monitors monitors = new Monitors();
    AdvertisedSocketAddress fromAddress = new AdvertisedSocketAddress( "localhost", 1234 );
    StoreId storeId = new StoreId( 1, 2, 3, 4 );

    LogProvider logProvider;
    FileSystemAbstraction fileSystemAbstraction;
    PageCache pageCache;
    HughStoreCopyClient hughStoreCopyClient;
    HughTxPullClient txPullClient;
    TransactionLogCatchUpWriter writer;
    TransactionLogCatchUpFactory transactionLogCatchUpFactory;

    HughRemoteStore subject;

    @Before
    public void setup()
    {
        logProvider = NullLogProvider.getInstance();
        fileSystemAbstraction = mock( FileSystemAbstraction.class );
        pageCache = mock( PageCache.class );
        hughStoreCopyClient = mock( HughStoreCopyClient.class );
        txPullClient = mock( HughTxPullClient.class );
        writer = mock( TransactionLogCatchUpWriter.class );
        transactionLogCatchUpFactory = factory( writer );
        monitors = new Monitors();
        subject =
                new HughRemoteStore( logProvider, fileSystemAbstraction, pageCache, hughStoreCopyClient, txPullClient, transactionLogCatchUpFactory, monitors );
    }

    @Test
    public void shouldCopyStoreFilesAndPullTransactions() throws Exception
    {
        // given
        when( txPullClient.pullTransactions( eq( fromAddress ), eq( storeId ), anyLong(), any() ) ).thenReturn(
                new TxPullRequestResult( SUCCESS_END_OF_STREAM, 13 ) );

        // when
        subject.copy( fromAddress, storeId, new File( "destination" ) );

        // then
        verify( hughStoreCopyClient ).copyStoreFiles( eq( fromAddress ), eq( storeId ), any( StoreFileStreams.class ) );
        verify( txPullClient ).pullTransactions( eq( fromAddress ), eq( storeId ), anyLong(), any( TxPullResponseListener.class ) );
    }

    @Test
    public void shouldSetLastPulledTransactionId() throws Exception
    {
        // given
        long lastFlushedTxId = 12;
        StoreId wantedStoreId = new StoreId( 4, 2, 1, 44 );

        when( hughStoreCopyClient.copyStoreFiles( eq( fromAddress ), eq( wantedStoreId ), any( StoreFileStreams.class ) ) ).thenReturn( lastFlushedTxId );

        when( txPullClient.pullTransactions( eq( fromAddress ), eq( wantedStoreId ), anyLong(), any( TxPullResponseListener.class ) ) ).thenReturn(
                new TxPullRequestResult( SUCCESS_END_OF_STREAM, 13 ) );

        // when
        subject.copy( fromAddress, wantedStoreId, new File( "destination" ) );

        // then
        long previousTxId = lastFlushedTxId - 1; // the interface is defined as asking for the one preceding
        verify( txPullClient ).pullTransactions( eq( fromAddress ), eq( wantedStoreId ), eq( previousTxId ), any( TxPullResponseListener.class ) );
    }

    @Test
    public void shouldCloseDownTxLogWriterIfTxStreamingFails() throws Exception
    {
        // given
        StoreId storeId = new StoreId( 1, 2, 3, 4 );

        doThrow( StoreCopyFailedException.class ).when( txPullClient ).pullTransactions( any( AdvertisedSocketAddress.class ), eq( storeId ), anyLong(),
                any( TransactionLogCatchUpWriter.class ) );

        // when
        executionIsExpectedToFail( () -> subject.copy( null, storeId, null ), StoreCopyFailedException.class );

        // then
        verify( writer ).close();
    }

    private TransactionLogCatchUpFactory factory( TransactionLogCatchUpWriter writer )
    {
        TransactionLogCatchUpFactory factory = mock( TransactionLogCatchUpFactory.class );
        try
        {
            when( factory.create( any( File.class ), any( FileSystemAbstraction.class ), any( PageCache.class ), any( LogProvider.class ), anyLong(),
                    anyBoolean() ) ).thenReturn( writer );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Mock construction is not supposed to fail", e );
        }
        return factory;
    }
}
