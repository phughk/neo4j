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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.catchup.storecopy.FileChunk;
import org.neo4j.causalclustering.catchup.storecopy.FileHeader;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import org.neo4j.causalclustering.catchup.storecopy.StoreFileStreams;
import org.neo4j.logging.Log;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.verify;

public class CopyStoreFilesResponseAdaptorTest
{
    CompletableFuture<Long> anyCompletableFuture = mock( CompletableFuture.class );

    Log log;
    StoreFileStreams storeFileStreams;

    CopyStoreFilesResponseAdaptor subject;

    @Before
    public void setup()
    {
        log = mock( Log.class );
        storeFileStreams = mock( StoreFileStreams.class );
        subject = new CopyStoreFilesResponseAdaptor( storeFileStreams, log );
    }

    @Test
    public void receivingFileChunkOtherThanLastSignalsNotEnd() throws IOException
    {
        // given
        FileChunk fileChunk = mock( FileChunk.class );
        stub( fileChunk.isLast() ).toReturn( false );

        // when
        boolean isLast = subject.onFileContent( anyCompletableFuture, fileChunk );

        // then
        assertFalse( isLast );
    }

    @Test
    public void receivingLastFileChunkSignalsEnd() throws IOException
    {
        // given
        FileChunk fileChunk = mock( FileChunk.class );
        stub( fileChunk.isLast() ).toReturn( true );

        // when
        boolean isLast = subject.onFileContent( anyCompletableFuture, fileChunk );

        // then
        assertTrue( isLast );
    }

    @Test
    public void receivingFileContentIsWrittenToStoreStream() throws IOException
    {
        // given
        byte[] bytes = "Some sort of payload".getBytes();

        // and
        FileChunk fileChunk = mock( FileChunk.class );
        stub( fileChunk.bytes() ).toReturn( bytes );

        // when
        subject.onFileContent( anyCompletableFuture, fileChunk );

        // then
        verify( storeFileStreams ).write( anyString(), anyInt(), eq( bytes ) );
    }

    @Test
    public void fileDetailsFromHeaderAreUsedWhenStoringContent() throws IOException
    {
        // given
        String destination = "target file name";
        int requiredAlignment = 123;
        FileHeader fileHeader = new FileHeader( destination, requiredAlignment );

        // and
        FileChunk fileChunk = mock( FileChunk.class );

        // when
        subject.onFileHeader( anyCompletableFuture, fileHeader );
        subject.onFileContent( anyCompletableFuture, fileChunk );

        // then
        verify( storeFileStreams ).write( eq( destination ), eq( requiredAlignment ), any() );
    }

    @Test
    public void fileStreamingCompletionMarkedWithCompleteSignalAndLastTransaction()
    {
        // given
        CompletableFuture<Long> signalListener = mock( CompletableFuture.class );

        // and
        long lastTransactionId = 531L;
        StoreCopyFinishedResponse storeCopyFinishedResponse = new StoreCopyFinishedResponse( StoreCopyFinishedResponse.Status.SUCCESS, lastTransactionId );

        // when
        subject.onFileStreamingComplete( signalListener, storeCopyFinishedResponse );

        // then
        verify( signalListener ).complete( lastTransactionId );
    }

    @Test
    public void uponCompletionFilenameIsLogged()
    {
        // given
        String filename = "filename to store to";
        FileHeader fileHeader = new FileHeader( filename );

        // and
        StoreCopyFinishedResponse anyStoreCopyFinishedResponse = mock( StoreCopyFinishedResponse.class );

        // when
        subject.onFileHeader( anyCompletableFuture, fileHeader );
        subject.onFileStreamingComplete( anyCompletableFuture, anyStoreCopyFinishedResponse );

        // then
        verify( log ).info( contains( filename ) );
    }
}
