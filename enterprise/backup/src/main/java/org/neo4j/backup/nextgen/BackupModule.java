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
import java.nio.file.Path;
import java.time.Clock;
import java.util.Optional;

import org.neo4j.backup.CommandFailedSupplier;
import org.neo4j.backup.OnlineBackupCommand;
import org.neo4j.backup.OnlineBackupConfigurationOverride;
import org.neo4j.backup.OnlineBackupRequiredArguments;
import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.causalclustering.catchup.tx.TxPullClient;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.com.storecopy.ExternallyManagedPageCache;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.TimeUtil;
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
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ssl.SslPolicy;

import static org.neo4j.kernel.impl.util.Converters.toHostnamePort;

public class BackupModule
{
    private final LogProvider logProvider;
    private final PageCache pageCache;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final StoreCopyClient storeCopyClient;
    private final CatchUpClient catchUpClient;
    private final Clock clock;
    private final long inactivityTimeoutMillis = 500;
    private final Monitors monitors;
    private final SslPolicy sslPolicy;

    public BackupModule( CommandFailedSupplier<Config> configSupplier, LogProvider logProvider ) throws CommandFailed
    {
        Config config = configSupplier.get();
        this.logProvider = logProvider;
        fileSystemAbstraction = new DefaultFileSystemAbstraction();
        pageCache = aPageCache( config, fileSystemAbstraction );
        clock = Clock.systemDefaultZone();
        monitors = new Monitors();
        sslPolicy = sslPolicy( config, logProvider );
        catchUpClient = catchUpClient();
        storeCopyClient = new StoreCopyClient( catchUpClient, logProvider );
    }

    public BackupDelegator backupDelegator( File storeDir, AdvertisedSocketAddress fromAddress )
    {
        return backupDelegator( storeCopyService( storeDir, fromAddress ) );
    }

    public BackupDelegator backupDelegator( StoreCopyService storeCopyService )
    {
        return new BackupDelegator( remoteStore(), catchUpClient, storeCopyClient, storeCopyService,
                ExternallyManagedPageCache.graphDatabaseFactoryWithPageCache( pageCache ), new ClearIdService( new IdGeneratorWrapper() ) );
    }

    public StoreCopyService storeCopyService( File storeDir, AdvertisedSocketAddress fromAddress )
    {
        return new StoreCopyService( storeDir, fileSystemAbstraction, pageCache, storeCopyClient, fromAddress );
    }

    private static PageCache aPageCache( Config config, FileSystemAbstraction fileSystemAbstraction )
    {
        PageCursorTracerSupplier pageCursorTracerSupplier = new DefaultPageCursorTracerSupplier();
        PageCacheTracer pageCacheTracer = new DefaultPageCacheTracer();
        int cachePageSize = 8192;
        int maxPages = 1024;
        PageSwapperFactory pageSwapperFactory = new SingleFilePageSwapperFactory();
        pageSwapperFactory.open( fileSystemAbstraction, config );
        return new MuninnPageCache( pageSwapperFactory, maxPages, cachePageSize, pageCacheTracer, pageCursorTracerSupplier );
    }

    private RemoteStore remoteStore()
    {
        Monitors monitors = new Monitors();
        TxPullClient txPullClient = new TxPullClient( catchUpClient, monitors );
        TransactionLogCatchUpFactory txLogFactory = new TransactionLogCatchUpFactory();
        LogProvider logProvider = NullLogProvider.getInstance();
        return new RemoteStore( logProvider, fileSystemAbstraction, pageCache, storeCopyClient, txPullClient, txLogFactory, monitors );
    }

    public FileSystemAbstraction fileSystemAbstraction()
    {
        return fileSystemAbstraction;
    }

    private static SslPolicy sslPolicy( Config config, LogProvider logProvider )
    {
        SslPolicyLoader sslPolicyLoader = SslPolicyLoader.create( config, logProvider );
        return sslPolicyLoader.getPolicy( config.get( CausalClusteringSettings.ssl_policy ) );
    }

    private CatchUpClient catchUpClient()
    {
        return new CatchUpClient( logProvider, clock, inactivityTimeoutMillis, monitors, sslPolicy );
    }

    public OnlineBackupConfigurationOverride establishConfigurationOverride( Config config, Arguments arguments ) throws IncorrectUsage
    {
        boolean checkGraph;
        boolean checkIndexes;
        boolean checkLabelScanStore;
        boolean checkPropertyOwners;
        try
        {
            // We can remove the loading from config file in 4.0
            if ( arguments.has( "cc-graph" ) )
            {
                checkGraph = arguments.getBoolean( "cc-graph" );
            }
            else
            {
                checkGraph = ConsistencyCheckSettings.consistency_check_graph.from( config );
            }
            if ( arguments.has( "cc-indexes" ) )
            {
                checkIndexes = arguments.getBoolean( "cc-indexes" );
            }
            else
            {
                checkIndexes = ConsistencyCheckSettings.consistency_check_indexes.from( config );
            }
            if ( arguments.has( "cc-label-scan-store" ) )
            {
                checkLabelScanStore = arguments.getBoolean( "cc-label-scan-store" );
            }
            else
            {
                checkLabelScanStore = ConsistencyCheckSettings.consistency_check_label_scan_store.from( config );
            }
            if ( arguments.has( "cc-property-owners" ) )
            {
                checkPropertyOwners = arguments.getBoolean( "cc-property-owners" );
            }
            else
            {
                checkPropertyOwners = ConsistencyCheckSettings.consistency_check_property_owners.from( config );
            }
            return new OnlineBackupConfigurationOverride( checkGraph, checkIndexes, checkLabelScanStore, checkPropertyOwners );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }
    }

    public OnlineBackupRequiredArguments establishRequiredArguments( String[] args, Arguments arguments ) throws IncorrectUsage
    {
        try
        {
            HostnamePort address = toHostnamePort( new HostnamePort( "localhost", 6362 ) ).apply( arguments.parse( args ).get( "from" ) );
            Path folder = arguments.getMandatoryPath( "backup-dir" );
            String name = arguments.get( "name" );
            boolean fallbackToFull = arguments.getBoolean( "fallback-to-full" );
            boolean doConsistencyCheck = arguments.getBoolean( "check-consistency" );
            long timeout = arguments.get( "timeout", TimeUtil.parseTimeMillis );
            Optional<Path> additionalConfig = arguments.getOptionalPath( "additional-config" );
            Path reportDir = arguments.getOptionalPath( "cc-report-dir" ).orElseThrow( () -> new IllegalArgumentException( "cc-report-dir must be a path" ) );
            return new OnlineBackupRequiredArguments( address, folder, name, fallbackToFull, doConsistencyCheck, timeout, additionalConfig, reportDir );
        }
        catch ( IllegalArgumentException e )
        {
            throw new IncorrectUsage( e.getMessage() );
        }
    }
}
