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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.catchup.CatchUpResponseAdaptor;
import org.neo4j.causalclustering.catchup.storecopy.FileChunk;
import org.neo4j.causalclustering.catchup.storecopy.FileHeader;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import org.neo4j.causalclustering.catchup.storecopy.StoreFileStreams;
import org.neo4j.logging.Log;

import static java.lang.String.format;

public class CopyStoreFilesResponseAdaptor extends CatchUpResponseAdaptor<Long>
{
    private final StoreFileStreams storeFileStreams;
    private final Log log;

    private String destination;
    private int requiredAlignment;

    public CopyStoreFilesResponseAdaptor( StoreFileStreams storeFileStreams, Log log )
    {
        this.storeFileStreams = storeFileStreams;
        this.log = log;
    }

    @Override
    public void onFileHeader( CompletableFuture<Long> requestOutcomeSignal, FileHeader fileHeader )
    {
        this.destination = fileHeader.fileName();
        this.requiredAlignment = fileHeader.requiredAlignment();
    }

    @Override
    public boolean onFileContent( CompletableFuture<Long> signal, FileChunk fileChunk ) throws IOException
    {
        storeFileStreams.write( destination, requiredAlignment, fileChunk.bytes() );
        return fileChunk.isLast();
    }

    @Override
    public void onFileStreamingComplete( CompletableFuture<Long> signal, StoreCopyFinishedResponse response )
    {
        log.info( format( "Finished streaming %s", destination ) );
        signal.complete( response.lastCommittedTxBeforeStoreCopy() );
    }
}
