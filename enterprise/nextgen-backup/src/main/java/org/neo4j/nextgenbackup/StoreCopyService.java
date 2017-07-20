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

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.storecopy.StoreFileStreams;
import org.neo4j.causalclustering.catchup.storecopy.StreamToDisk;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.monitoring.Monitors;

public class StoreCopyService
{
    private final File storeDir;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final PageCache pageCache;
    private final HughStoreCopyClient hughStoreCopyClient;
    private final AdvertisedSocketAddress fromAddress;

    public StoreCopyService( File storeDir, FileSystemAbstraction fileSystemAbstraction, PageCache pageCache, HughStoreCopyClient hughStoreCopyClient,
            AdvertisedSocketAddress fromAddress )
    {
        this.storeDir = storeDir;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.pageCache = pageCache;
        this.hughStoreCopyClient = hughStoreCopyClient;
        this.fromAddress = fromAddress;
    }

    public long retrieveStore( StoreFileStreams storeFileStreams, StoreId expectedStoreId )
    {
        try
        {
            long latestTxNumber = hughStoreCopyClient.copyStoreFiles( fromAddress, expectedStoreId, storeFileStreams );
            storeFileStreams.close();
            return latestTxNumber;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    public StoreFileStreams constructStoreFileStreams() // TODO potentially move 3 params
    {
//        Monitors monitors = new Monitors();
//        return new StreamToDisk( storeDir, fileSystemAbstraction, pageCache, monitors );
        return null;
    }
}
