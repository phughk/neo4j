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
import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.catchup.CatchUpResponseAdaptor;
import org.neo4j.causalclustering.catchup.CatchUpResponseCallback;
import org.neo4j.causalclustering.catchup.HughCatchUpClient;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import org.neo4j.causalclustering.catchup.storecopy.StoreFileStreams;
import org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.causalclustering.catchup.storecopy.StreamToDisk;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.com.storecopy.ExternallyManagedPageCache;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

public class Application implements Runnable
{
    HughCatchUpClient hughCatchUpClient;
    HughStoreCopyClient hughStoreCopyClient;
    CatchUpResponseCallback responseHandler;
    HughRemoteStore remoteStore;
    private PageCache pageCache;
    private final File storeDir = new File( "deleteThis/" );
    private static final FileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();

    public Application()
    {
        responseHandler = new CatchUpResponseAdaptor()
        {
            @Override
            public void onFileStreamingComplete( CompletableFuture signal, StoreCopyFinishedResponse response )
            {
                System.out.println( "Application.hughCatchUpClient onFileStreamingComplete - FILE STREAMING COMPLETE" );
                signal.complete( null );
            }
        };
        pageCache = aPageCache();
        hughCatchUpClient = HughCatchUpClient.withDefaults( new Monitors(), responseHandler );
        hughStoreCopyClient = new HughStoreCopyClient( hughCatchUpClient, NullLogProvider.getInstance() );
        remoteStore = remoteStore();
    }

    public static void main( String[] args )
    {
        Application application = new Application();
        application.run();
    }

    private void start()
    {
        hughCatchUpClient.start();
    }

    private void stop()
    {
        hughCatchUpClient.stop();
    }

    private HughRemoteStore remoteStore()
    {
        Monitors monitors = new Monitors();
        HughTxPullClient txPullClient = new HughTxPullClient( hughCatchUpClient, monitors );
        TransactionLogCatchUpFactory txLogFactory = new TransactionLogCatchUpFactory();
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        LogProvider logProvider = NullLogProvider.getInstance();
        return new HughRemoteStore( logProvider, fs, pageCache, hughStoreCopyClient, txPullClient, txLogFactory, monitors );
    }

    private long retrieveStore( StoreId expectedStoreId )
    {
        try
        {
            Monitors monitors = new Monitors();
            StoreFileStreams storeFileStreams = new StreamToDisk( storeDir, fileSystemAbstraction, pageCache, monitors );
            // store id pulled from instance for which to use the backup
            AdvertisedSocketAddress fromAddress = fromAddress();
            long latestTxNumber = hughStoreCopyClient.copyStoreFiles( fromAddress, expectedStoreId, storeFileStreams );
            storeFileStreams.close();
            return latestTxNumber;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private static AdvertisedSocketAddress fromAddress()
    {
        int backupPort = 6362;
        int causalTxListen = 6000;
        int causalRaftListen = 7000;
        int netcat = 6004;

        return new AdvertisedSocketAddress( "127.0.0.1", causalTxListen );
    }

    private static PageCache aPageCache()
    {
        PageCursorTracerSupplier pageCursorTracerSupplier = new DefaultPageCursorTracerSupplier();
        PageCacheTracer pageCacheTracer = new DefaultPageCacheTracer();
        int cachePageSize = 8192;
        int maxPages = 1024;
        PageSwapperFactory pageSwapperFactory = new SingleFilePageSwapperFactory();
        pageSwapperFactory.open( fileSystemAbstraction, Configuration.EMPTY );
        return new MuninnPageCache( pageSwapperFactory, maxPages, cachePageSize, pageCacheTracer, pageCursorTracerSupplier );
    }

    @Override
    public void run()
    {
        start();
        try
        {
            StoreId expectedStoreId = expectedStoreId();
            long latestTxNumber = retrieveStore( expectedStoreId );
            remoteStore.tryCatchingUp( fromAddress(), expectedStoreId, storeDir );
            clearIdFiles( fileSystemAbstraction, storeDir );
        }
        finally
        {
            stop();
        }
        fixLocalStorage();
    }

    private void fixLocalStorage()
    {
        try
        {
            pageCache.flushAndForce();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        System.out.println("Starting graph database");
        GraphDatabaseFactory graphDatabaseFactory = ExternallyManagedPageCache.graphDatabaseFactoryWithPageCache( pageCache );
        GraphDatabaseService graphDatabaseService = graphDatabaseFactory.newEmbeddedDatabaseBuilder( storeDir ).setConfig( GraphDatabaseSettings.label_index,
                GraphDatabaseSettings.LabelIndex.AUTO.name() ).setConfig( "dbms.backup.enabled", Settings.FALSE ).setConfig(
                GraphDatabaseSettings.logs_directory, storeDir.getAbsolutePath() ).setConfig( GraphDatabaseSettings.keep_logical_logs,
                Settings.TRUE ).newGraphDatabase();
        System.out.println( "Stopping graph database" );
        graphDatabaseService.shutdown();
    }

    private StoreId expectedStoreId()
    {
        AdvertisedSocketAddress fromAddress = fromAddress();
        try
        {
            return hughStoreCopyClient.fetchStoreId( fromAddress );
        }
        catch ( StoreIdDownloadFailedException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void clearIdFiles( FileSystemAbstraction fileSystem, File targetDirectory )
    {
        try
        {
            for ( File file : fileSystem.listFiles( targetDirectory ) )
            {
                if ( !fileSystem.isDirectory( file ) && file.getName().endsWith( ".id" ) )
                {
                    long highId = IdGeneratorImpl.readHighId( fileSystem, file );
                    fileSystem.deleteFile( file );
                    IdGeneratorImpl.createGenerator( fileSystem, file, highId, true );
                }
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
