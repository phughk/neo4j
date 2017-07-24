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
package org.neo4j.backup.nextgen;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.verify;

public class BackupDelegatorTest
{
    RemoteStore hughRemoteStore;
    CatchUpClient hughCatchUpClient;
    StoreCopyClient hughStoreCopyClient;
    GraphDatabaseFactory graphDatabaseFactory;
    ClearIdService clearIdService;
    StoreCopyService storeCopyService;

    BackupDelegator subject;

    @Before
    public void setup()
    {
        hughRemoteStore = mock( RemoteStore.class );
        hughCatchUpClient = mock( CatchUpClient.class );
        hughStoreCopyClient = mock( StoreCopyClient.class );
        graphDatabaseFactory = mock( GraphDatabaseFactory.class );
        clearIdService = mock( ClearIdService.class );
        storeCopyService = mock( StoreCopyService.class );
        subject = new BackupDelegator( hughRemoteStore, hughCatchUpClient, hughStoreCopyClient, storeCopyService, graphDatabaseFactory, clearIdService );
    }

    @Test
    public void tryCatchingUpDelegatesToRemoteStore() throws org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException, IOException
    {
        // given
        AdvertisedSocketAddress fromAddress = new AdvertisedSocketAddress( "neo4j.com", 5432 );
        StoreId expectedStoreId = new StoreId( 7, 2, 5, 98 );
        File storeDir = new File( "A directory to store transactions to" );

        // when
        subject.tryCatchingUp( fromAddress, expectedStoreId, storeDir );

        // then
        verify( hughRemoteStore ).tryCatchingUp( fromAddress, expectedStoreId, storeDir );
    }

    @Test
    public void startDelegatesToCatchUpClient()
    {
        // when
        subject.start();

        // then
        verify( hughCatchUpClient ).start();
    }

    @Test
    public void stopDelegatesToCatchUpClient()
    {
        // when
        subject.stop();

        // then
        verify( hughCatchUpClient ).stop();
    }

    @Test
    public void fetchStoreIdDelegatesToStoreCopyClient() throws StoreIdDownloadFailedException
    {
        // given
        AdvertisedSocketAddress fromAddress = new AdvertisedSocketAddress( "neo4.com", 935 );

        // and
        StoreId expectedStoreId = new StoreId( 6, 2, 9, 3 );
        stub( hughStoreCopyClient.fetchStoreId( fromAddress ) ).toReturn( expectedStoreId );

        // when
        StoreId storeId = subject.fetchStoreId( fromAddress );

        // then
        assertEquals( expectedStoreId, storeId );
    }

    @Test
    public void retrieveStoreDelegatesToStoreCopyService()
    {
        // given
        StoreId storeId = new StoreId( 92, 5, 7, 32 );
        stub( storeCopyService.retrieveStore(any(), eq(storeId)) ).toReturn( 9142L );

        // when
        long actualTransactionId = subject.retrieveStore( storeId );

        // then
        assertEquals( 9142, actualTransactionId );
    }

    @Test
    public void clearIdFilesDelegatesToClearIdService()
    {
        // given
        FileSystemAbstraction fileSystemAbstraction = mock( FileSystemAbstraction.class );
        File file = new File( "any file name" );

        // when
        subject.clearIdFiles( fileSystemAbstraction, file );

        // then
        verify( clearIdService ).clearIdFiles( fileSystemAbstraction, file );
    }
}
