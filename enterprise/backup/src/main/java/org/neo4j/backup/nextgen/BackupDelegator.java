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

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Settings;

public class BackupDelegator
{
    private final GraphDatabaseFactory graphDatabaseFactory;
    private final RemoteStore remoteStore;
    private final CatchUpClient catchUpClient;
    private final StoreCopyClient storeCopyClient;
    private final StoreCopyService storeCopyService;
    private final ClearIdService clearIdService;

    public BackupDelegator( RemoteStore remoteStore, CatchUpClient catchUpClient, StoreCopyClient storeCopyClient, StoreCopyService storeCopyService,
            GraphDatabaseFactory graphDatabaseFactory, ClearIdService clearIdService )
    {
        this.remoteStore = remoteStore;
        this.catchUpClient = catchUpClient;
        this.storeCopyClient = storeCopyClient;
        this.storeCopyService = storeCopyService;
        this.graphDatabaseFactory = graphDatabaseFactory;
        this.clearIdService = clearIdService;
    }

    public void fixLocalStorage( File storeDir )
    {
        GraphDatabaseService graphDatabaseService = graphDatabaseFactory.newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.label_index, GraphDatabaseSettings.LabelIndex.AUTO.name() )
                .setConfig( "dbms.backup.enabled", Settings.FALSE )
                .setConfig( GraphDatabaseSettings.logs_directory, storeDir.getAbsolutePath() )
                .setConfig( GraphDatabaseSettings.keep_logical_logs, Settings.TRUE ).newGraphDatabase();
        graphDatabaseService.shutdown();
    }

    public CatchupResult tryCatchingUp( AdvertisedSocketAddress fromAddress, StoreId expectedStoreId, File storeDir )
    {
        try
        {
            return remoteStore.tryCatchingUp( fromAddress, expectedStoreId, storeDir );
        }
        catch ( org.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException | IOException e )
        {
            e.printStackTrace();
        }
        return CatchupResult.E_STORE_UNAVAILABLE; // TODO resolve this
    }

    public void start()
    {
        catchUpClient.start();
    }

    public void stop()
    {
        catchUpClient.stop();
    }

    public StoreId fetchStoreId( AdvertisedSocketAddress fromAddress ) throws StoreIdDownloadFailedException
    {
        return storeCopyClient.fetchStoreId( fromAddress );
    }

    public long retrieveStore( StoreId expectedStoreId )
    {
        return storeCopyService.retrieveStore( storeCopyService.constructStoreFileStreams(), expectedStoreId );
    }

    public void clearIdFiles( FileSystemAbstraction fileSystem, File targetDirectory )
    {
        clearIdService.clearIdFiles( fileSystem, targetDirectory );
    }
}
