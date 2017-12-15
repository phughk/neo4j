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
package org.neo4j.causalclustering;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchupServer;
import org.neo4j.causalclustering.core.consensus.LeaderAvailabilityHandler;
import org.neo4j.causalclustering.core.consensus.LeaderAvailabilityTimers;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.neo4j.causalclustering.core.consensus.membership.RaftMembershipManager;
import org.neo4j.causalclustering.core.consensus.schedule.TimerService;
import org.neo4j.causalclustering.core.consensus.shipping.RaftLogShippingManager;
import org.neo4j.causalclustering.core.consensus.term.TermState;
import org.neo4j.causalclustering.core.consensus.vote.VoteState;
import org.neo4j.causalclustering.core.state.CommandApplicationProcess;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.core.state.CoreState;
import org.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import org.neo4j.causalclustering.core.state.storage.StateStorage;
import org.neo4j.causalclustering.discovery.CoreTopologyService;
import org.neo4j.causalclustering.discovery.SharedDiscoveryCoreClient;
import org.neo4j.causalclustering.discovery.SharedDiscoveryService;
import org.neo4j.causalclustering.handlers.NoOpPipelineHandlerAppender;
import org.neo4j.causalclustering.handlers.PipelineHandlerAppender;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.Message;
import org.neo4j.causalclustering.messaging.Outbound;
import org.neo4j.causalclustering.messaging.RaftChannelInitializer;
import org.neo4j.causalclustering.messaging.RaftOutbound;
import org.neo4j.causalclustering.messaging.SenderService;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

public class CatchupServerBuilder
{
    public CatchupServer catchupServer;
    public PipelineHandlerAppender pipelineHandlerAppender;
    public StoreCopyCheckPointMutex storeCopyCheckPointMutex;
    public PageCache pageCache;
    public FileSystemAbstraction fileSystemAbstraction;
    public Supplier<CheckPointer> checkPointerSupplier;
    public Monitors monitors;
    public CoreSnapshotService coreSnapshotService;
    public BooleanSupplier dataSourceAvailabilitySupplier;
    public Supplier<NeoStoreDataSource> dataSourceSupplier;
    public Supplier<LogicalTransactionStore> logicalTransactionStoreSupplier;
    public Supplier<TransactionIdStore> transactionIdStoreSupplier;
    public Supplier<StoreId> storeIdSupplier;
    public Config config;
    public LogProvider userLogProvider;
    public LogProvider logProvider;
    public CommandApplicationProcess commandApplicationProcess;
    public CoreState coreState;
    private InMemoryRaftLog raftLog;
    private RaftMachine raftMachine;
    private MemberId memberId;
    private InMemoryStateStorage<TermState> termStorage;
    private InMemoryStateStorage<VoteState> voteStorage;
    private LeaderAvailabilityTimers leaderAvailabilityTimers;
    private Duration electionTimeout;
    private Duration heartbeatInterval;
    private Clock clock;
    private TimerService timerService;
    private JobScheduler jobScheduler;
    private Outbound<MemberId,RaftMessages.RaftMessage> outboundRaftMessages;
    private CoreTopologyService coreTopologyService;
    private SharedDiscoveryService sharedDiscoveryService;
    private Outbound<AdvertisedSocketAddress,Message> outboundSocketMessages;
    private Supplier<Optional<ClusterId>> clusterIdentity;
    private Long logThresholdMillis;
    private ChannelInitializer<SocketChannel> channelInitializer;

    public CatchupServer getCatchupServer() // TODO build
    {
        if ( catchupServer == null )
        {
            catchupServer = new CatchupServer( getLogProvider(), getUserLogProvider(), getStoreIdSupplier(), getTransactionIdStoreSupplier(),
                    getLogicalTransactionStoreSupplier(), getDataSourceSupplier(), getDataSourceAvailabilitySupplier(), getCoreSnapshotService(), getConfig(),
                    getMonitors(), getCheckPointerSupplier(), getFileSystemAbstraction(), getPageCache(), getStoreCopyCheckPointMutex(),
                    getPipelineHandlerAppender() );
        }
        return catchupServer;
    }

    private PipelineHandlerAppender getPipelineHandlerAppender()
    {
        if ( pipelineHandlerAppender == null )
        {
            pipelineHandlerAppender = new NoOpPipelineHandlerAppender( getConfig(), getLogProvider() );
        }
        return pipelineHandlerAppender;
    }

    private StoreCopyCheckPointMutex getStoreCopyCheckPointMutex()
    {
        if ( storeCopyCheckPointMutex == null )
        {
            storeCopyCheckPointMutex = new StoreCopyCheckPointMutex();
        }
        return storeCopyCheckPointMutex;
    }

    private PageCache getPageCache()
    {
        if ( pageCache == null )
        {
            pageCache = ConfigurableStandalonePageCacheFactory.createPageCache( getFileSystemAbstraction(), getConfig() );
        }
        return pageCache;
    }

    private FileSystemAbstraction getFileSystemAbstraction()
    {
        if ( fileSystemAbstraction == null )
        {
            fileSystemAbstraction = new DefaultFileSystemAbstraction();
        }
        return fileSystemAbstraction;
    }

    private Supplier<CheckPointer> getCheckPointerSupplier()
    {
        if ( checkPointerSupplier == null )
        {
            checkPointerSupplier = () -> null; // TODO change
        }
        return checkPointerSupplier;
    }

    private Monitors getMonitors()
    {
        if ( monitors == null )
        {
            monitors = new Monitors();
        }
        return monitors;
    }

    private CoreSnapshotService getCoreSnapshotService()
    {
        if ( coreSnapshotService == null )
        {
            coreSnapshotService = new CoreSnapshotService( getApplicationProcess(), getCoreState(), getRaftLog(), getRaftMachine() );
        }
        return coreSnapshotService;
    }

    private RaftMachine getRaftMachine()
    {
        if ( raftMachine == null )
        {
            raftMachine =
                    new RaftMachine( getMemberId(), getTermStorage(), getVoteStorage(), getRaftLog(), getLeaderAvailabilityTimers(), getOutboundRaftMessages(),
                            getLogProvider(), getRaftMembershipManager(), getRaftLogShippingManager(), getInFlightCache(), getRefuseToBecomeLeader(),
                            getSupportPreVoting(), getMonitors() );
        }
        return raftMachine;
    }

    private MemberId getMemberId()
    {
        if ( memberId == null )
        {
            memberId = new MemberId( UUID.randomUUID() );
        }
        return memberId;
    }

    private StateStorage<TermState> getTermStorage()
    {
        if ( termStorage == null )
        {
            termStorage = new InMemoryStateStorage<>( new TermState() );
        }
        return termStorage;
    }

    private StateStorage<VoteState> getVoteStorage()
    {
        if ( voteStorage == null )
        {
            voteStorage = new InMemoryStateStorage<>( new VoteState() );
        }
        return voteStorage;
    }

    private LeaderAvailabilityTimers getLeaderAvailabilityTimers()
    {
        if ( leaderAvailabilityTimers == null )
        {
            leaderAvailabilityTimers =
                    new LeaderAvailabilityTimers( getElectionTimeout(), getHeartbeatInterval(), getClock(), getTimerService(), getLogProvider() );
        }
        return leaderAvailabilityTimers;
    }

    private Duration getElectionTimeout()
    {
        if ( electionTimeout == null )
        {
            electionTimeout = Duration.ofSeconds( 10 );
        }
        return electionTimeout;
    }

    private Duration getHeartbeatInterval()
    {
        if ( heartbeatInterval == null )
        {
            heartbeatInterval = Duration.ofSeconds( 2 );
        }
        return heartbeatInterval;
    }

    private Clock getClock()
    {
        if ( clock == null )
        {
            clock = Clock.systemDefaultZone();
        }
        return clock;
    }

    private TimerService getTimerService()
    {
        if ( timerService == null )
        {
            timerService = new TimerService( getJobScheduler(), getLogProvider() );
        }
        return timerService;
    }

    private JobScheduler getJobScheduler()
    {
        if ( jobScheduler == null )
        {
            jobScheduler = new Neo4jJobScheduler();
        }
        return jobScheduler;
    }

    private Outbound<MemberId,RaftMessages.RaftMessage> getOutboundRaftMessages()
    {
        if ( outboundRaftMessages == null )
        {
            outboundRaftMessages =
                    new RaftOutbound( getCoreTopologyService(), getOutboundSocketMessages(), getClusterIdentity(), getLogProvider(), getLogThresholdMillis() );
        }
        return outboundRaftMessages;
    }

    private Long getLogThresholdMillis()
    {
        if ( logThresholdMillis == null )
        {
            logThresholdMillis = 1000L;
        }
        return logThresholdMillis;
    }

    private Supplier<Optional<ClusterId>> getClusterIdentity()
    {
        if ( clusterIdentity == null )
        {
            clusterIdentity = () -> Optional.of( new ClusterId( UUID.randomUUID() ) );
        }
        return clusterIdentity;
    }

    private Outbound<AdvertisedSocketAddress,Message> getOutboundSocketMessages()
    {
        if ( outboundSocketMessages == null )
        {
            outboundSocketMessages = new SenderService( getChannelInitializer(), getLogProvider(), getMonitors() );
        }
        return outboundSocketMessages;
    }

    private ChannelInitializer<SocketChannel> getChannelInitializer()
    {
        if (channelInitializer == null)
        {
                channelInitializer = new RaftChannelInitializer();
        }
        return channelInitializer;
    }

    private CoreTopologyService getCoreTopologyService()
    {
        if ( coreTopologyService == null )
        {
            coreTopologyService = new SharedDiscoveryCoreClient( getSharedDiscoveryService(), getMemberId(), getLogProvider(), getConfig() );
        }
        return coreTopologyService;
    }

    private SharedDiscoveryService getSharedDiscoveryService()
    {
        if ( sharedDiscoveryService == null )
        {
            sharedDiscoveryService = new SharedDiscoveryService();
        }
        return sharedDiscoveryService;
    }

    private RaftMembershipManager getRaftMembershipManager()
    {

    }

    private RaftLogShippingManager getRaftLogShippingManager()
    {

    }

    private InFlightCache getInFlightCache()
    {

    }

    private Boolean getRefuseToBecomeLeader()
    {

    }

    private Boolean getSupportPreVoting()
    {

    }

    private CoreState getCoreState()
    {
        if ( coreState == null )
        {
            coreState = new CoreState( getCoreStateMachines(), getSessionTracker, getLastFlushedStorage() );
        }
        return coreState;
    }

    private RaftLog getRaftLog()
    {
        if ( raftLog == null )
        {
            raftLog = new InMemoryRaftLog();
        }
        return raftLog;
    }

    private CommandApplicationProcess getApplicationProcess()
    {
        if ( commandApplicationProcess == null )
        {
            commandApplicationProcess =
                    new CommandApplicationProcess( getRaftLog(), getMaxBatchSize(), getFlushEvery(), getDbHealthSupplier(), getLogProvider(),
                            getProgressTracker(), getSession )
        }
    }

    private BooleanSupplier getDataSourceAvailabilitySupplier()
    {
        throw new RuntimeException( "Unimplemented" );
    }

    private Supplier<NeoStoreDataSource> getDataSourceSupplier()
    {
        throw new RuntimeException( "Unimplemented" );
    }

    private Supplier<LogicalTransactionStore> getLogicalTransactionStoreSupplier()
    {
        throw new RuntimeException( "Unimplemented" );
    }

    private Supplier<TransactionIdStore> getTransactionIdStoreSupplier()
    {
        throw new RuntimeException( "Unimplemented" );
    }

    private Supplier<StoreId> getStoreIdSupplier()
    {
        throw new RuntimeException( "Unimplemented" );
    }

    private Config getConfig()
    {
        throw new RuntimeException( "Unimplemented" );
    }

    private LogProvider getUserLogProvider()
    {
        throw new RuntimeException( "Unimplemented" );
    }

    private LogProvider getLogProvider()
    {
        throw new RuntimeException( "Unimplemented" );
    }
}
