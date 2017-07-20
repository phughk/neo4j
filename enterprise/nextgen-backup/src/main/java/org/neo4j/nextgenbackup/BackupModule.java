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

import org.neo4j.causalclustering.catchup.HughCatchUpClient;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.com.storecopy.ExternallyManagedPageCache;
import org.neo4j.graphdb.config.Configuration;
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
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

public class BackupModule
{
    private final PageCache pageCache;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final HughStoreCopyClient hughStoreCopyClient;
    private final HughCatchUpClient hughCatchUpClient;
    private final BackupDelegator backupDelegator;

    public BackupModule( File storeDir, AdvertisedSocketAddress fromAddress )
    {
        fileSystemAbstraction = new DefaultFileSystemAbstraction();
        pageCache = aPageCache( fileSystemAbstraction );
        hughCatchUpClient = HughCatchUpClient.withDefaults( new Monitors() );
        hughStoreCopyClient = new HughStoreCopyClient( hughCatchUpClient, NullLogProvider.getInstance() );
        StoreCopyService storeCopyService = new StoreCopyService( storeDir, fileSystemAbstraction, pageCache, hughStoreCopyClient, fromAddress );
        backupDelegator = new BackupDelegator( hughRemoteStore(), hughCatchUpClient, hughStoreCopyClient, storeCopyService,
                ExternallyManagedPageCache.graphDatabaseFactoryWithPageCache( pageCache ), new ClearIdService( new IdGeneratorDelegator() ) );
    }

    private static PageCache aPageCache( FileSystemAbstraction fileSystemAbstraction )
    {
        PageCursorTracerSupplier pageCursorTracerSupplier = new DefaultPageCursorTracerSupplier();
        PageCacheTracer pageCacheTracer = new DefaultPageCacheTracer();
        int cachePageSize = 8192;
        int maxPages = 1024;
        PageSwapperFactory pageSwapperFactory = new SingleFilePageSwapperFactory();
        pageSwapperFactory.open( fileSystemAbstraction, Configuration.EMPTY );
        return new MuninnPageCache( pageSwapperFactory, maxPages, cachePageSize, pageCacheTracer, pageCursorTracerSupplier );
    }

    private HughRemoteStore hughRemoteStore()
    {
        Monitors monitors = new Monitors();
        HughTxPullClient txPullClient = new HughTxPullClient( hughCatchUpClient, monitors );
        TransactionLogCatchUpFactory txLogFactory = new TransactionLogCatchUpFactory();
        LogProvider logProvider = NullLogProvider.getInstance();
        return new HughRemoteStore( logProvider, fileSystemAbstraction, pageCache, hughStoreCopyClient, txPullClient, txLogFactory, monitors );
    }

    public BackupDelegator databaseRestorationService()
    {
        return backupDelegator;
    }

    public FileSystemAbstraction fileSystemAbstraction()
    {
        return fileSystemAbstraction;
    }
}
