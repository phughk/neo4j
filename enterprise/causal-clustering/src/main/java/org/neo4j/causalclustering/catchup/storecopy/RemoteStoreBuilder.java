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
package org.neo4j.causalclustering.catchup.storecopy;

import java.time.Clock;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.tx.TransactionLogCatchUpFactory;
import org.neo4j.causalclustering.catchup.tx.TxPullClient;
import org.neo4j.causalclustering.handlers.NoOpPipelineHandlerAppender;
import org.neo4j.causalclustering.handlers.PipelineHandlerAppender;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.SystemNanoClock;

public class RemoteStoreBuilder
{
    public RemoteStore remoteStore;
    public LogProvider logProvider;
    public FileSystemAbstraction fileSystemAbstraction;
    public PageCache pageCache;
    public StoreCopyClient storeCopyClient;
    public TxPullClient txPullClient;
    public TransactionLogCatchUpFactory transactionLogCatchUpFactory;
    public Config config;
    public Monitors monitors;
    public CatchUpClient catchUpClient;
    public Clock clock;
    public Long inactivityTimeoutMillis;
    public PipelineHandlerAppender pipelineHandlerAppender;

    public RemoteStore getRemoteStore()
    {
        if ( remoteStore == null )
        {
            remoteStore = new RemoteStore( getLogProvider(), getFileSystemAbstraction(), getPageCache(), getStoreCopyClient(), getTxPullClient(),
                    getTransactionLogCatchUpFactory(), getConfig(), getMonitors() );
        }
        return remoteStore;
    }

    public LogProvider getLogProvider()
    {
        if ( logProvider == null )
        {
            logProvider = NullLogProvider.getInstance();
        }
        return logProvider;
    }

    public FileSystemAbstraction getFileSystemAbstraction()
    {
        if ( fileSystemAbstraction == null )
        {
            fileSystemAbstraction = new DefaultFileSystemAbstraction();
        }
        return fileSystemAbstraction;
    }

    public PageCache getPageCache()
    {
        if ( pageCache == null )
        {
            pageCache = ConfigurableStandalonePageCacheFactory.createPageCache( getFileSystemAbstraction(), getConfig() );
        }
        return pageCache;
    }

    public StoreCopyClient getStoreCopyClient()
    {
        if ( storeCopyClient == null )
        {
            storeCopyClient = new StoreCopyClient( getCatchUpClient(), getLogProvider() );
        }
        return storeCopyClient;
    }

    public TxPullClient getTxPullClient()
    {
        if ( txPullClient == null )
        {
            txPullClient = new TxPullClient( getCatchUpClient(), getMonitors() );
        }
        return txPullClient;
    }

    public TransactionLogCatchUpFactory getTransactionLogCatchUpFactory()
    {
        if ( transactionLogCatchUpFactory == null )
        {
            transactionLogCatchUpFactory = new TransactionLogCatchUpFactory();
        }
        return transactionLogCatchUpFactory;
    }

    public CatchUpClient getCatchUpClient()
    {
        if ( catchUpClient == null )
        {
            catchUpClient = new CatchUpClient( getLogProvider(), getClock(), getInactivityTimeoutMillis(), getMonitors(), getPipelineHandlerAppender() );
        }
        return catchUpClient;
    }

    public Monitors getMonitors()
    {
        if ( monitors == null )
        {
            monitors = new Monitors();
        }
        return monitors;
    }

    public Clock getClock()
    {
        if ( clock == null )
        {
            clock = SystemNanoClock.systemDefaultZone();
        }
        return clock;
    }

    public Long getInactivityTimeoutMillis()
    {
        if ( inactivityTimeoutMillis == null )
        {
            long minutes = 1000 * 60;
            inactivityTimeoutMillis = 10 * minutes;
        }
        return inactivityTimeoutMillis;
    }

    public PipelineHandlerAppender getPipelineHandlerAppender()
    {
        if ( pipelineHandlerAppender == null )
        {
            pipelineHandlerAppender = new NoOpPipelineHandlerAppender( getConfig(), getLogProvider() );
        }
        return pipelineHandlerAppender;
    }

    public Config getConfig()
    {
        if ( config == null )
        {
            config = Config.defaults();
        }
        return config;
    }
}
