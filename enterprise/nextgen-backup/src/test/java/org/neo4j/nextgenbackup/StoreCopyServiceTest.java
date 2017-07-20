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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.internal.InOrderImpl;

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.catchup.storecopy.StoreFileStreams;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StoreCopyServiceTest
{
    StoreId expectedStoreId = new StoreId( 1, 7, 2, 8 );
    StoreFileStreams expectedStoreFileStreams = mock( StoreFileStreams.class );

    File storeDir;
    FileSystemAbstraction fileSystemAbstraction;
    PageCache pageCache;
    HughStoreCopyClient hughStoreCopyClient;
    AdvertisedSocketAddress fromAddress;

    StoreCopyService subject;

    @Before
    public void setup()
    {
        storeDir = new File( "Store directory" );
        fileSystemAbstraction = mock( FileSystemAbstraction.class );
        pageCache = mock( PageCache.class );
        hughStoreCopyClient = mock( HughStoreCopyClient.class );
        fromAddress = new AdvertisedSocketAddress( "localhost", 1637 );
        subject = new StoreCopyService( storeDir, fileSystemAbstraction, pageCache, hughStoreCopyClient, fromAddress );
    }

    @Test
    public void latestTransactionNumberIsThatFromStoreCopyClient()
    {
        // given
        long expectedLastTransactionId = 16378;
        stub( hughStoreCopyClient.copyStoreFiles( any(), any(), any() ) ).toReturn( expectedLastTransactionId );

        // when
        long result = subject.retrieveStore( expectedStoreFileStreams, expectedStoreId );

        // then
        verify( hughStoreCopyClient ).copyStoreFiles( fromAddress, expectedStoreId, expectedStoreFileStreams );
        assertEquals( expectedLastTransactionId, result );
    }

    @Test
    public void storeFilesStreamIsClosedAfterStoreCopy() throws Exception
    {
        // when
        subject.retrieveStore( expectedStoreFileStreams, expectedStoreId );

        // then
        InOrder inOrder = Mockito.inOrder( hughStoreCopyClient, expectedStoreFileStreams );
        inOrder.verify( hughStoreCopyClient ).copyStoreFiles( fromAddress, expectedStoreId, expectedStoreFileStreams );
        inOrder.verify( expectedStoreFileStreams ).close();
    }

    @Test
    public void storeFileStreamsExceptionsAreWrappedInUnchecked() throws Exception
    {
        // given
        doThrow( IOException.class ).when( expectedStoreFileStreams ).close();

        // when
        RuntimeException runtimeException = null;
        try
        {
            subject.retrieveStore( expectedStoreFileStreams, expectedStoreId );
        }
        catch ( RuntimeException e )
        {
            runtimeException = e;
        }

        // then
        assertNotNull( runtimeException );
        assertEquals( IOException.class, runtimeException.getCause().getClass() );
    }
}
