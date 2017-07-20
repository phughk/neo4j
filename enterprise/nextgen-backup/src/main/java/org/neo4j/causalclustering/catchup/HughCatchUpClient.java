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
package org.neo4j.causalclustering.catchup;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.messaging.CatchUpRequest;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ssl.SslPolicy;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.neo4j.causalclustering.catchup.TimeoutLoop.waitForCompletion;

public class HughCatchUpClient extends LifecycleAdapter
{

    private final LogProvider logProvider;
    private final Log log;
    private final Clock clock;
    private final Monitors monitors;
    private final SslPolicy sslPolicy;
    private final long inactivityTimeoutMillis;
    private final CatchUpChannelPool<CatchUpChannel> pool;

    private NioEventLoopGroup eventLoopGroup;

    public static HughCatchUpClient withDefaults( Monitors monitors )
    {
        LogProvider logProvider = NullLogProvider.getInstance();
        SslPolicyLoader sslPolicyLoader = SslPolicyLoader.create( Config.defaults(), logProvider );
        Config config = Config.defaults();
        SslPolicy sslPolicy = sslPolicyLoader.getPolicy( config.get( CausalClusteringSettings.ssl_policy ) );
        return new HughCatchUpClient( logProvider, Clock.systemDefaultZone(), 1 * 1000, monitors, sslPolicy );
    }

    public HughCatchUpClient( LogProvider logProvider, Clock clock, long inactivityTimeoutMillis, Monitors monitors, SslPolicy sslPolicy )
    {
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
        this.clock = clock;
        this.inactivityTimeoutMillis = inactivityTimeoutMillis;
        this.monitors = monitors;
        this.sslPolicy = sslPolicy;
        pool = new CatchUpChannelPool<>( addr -> new CatchUpChannel( addr, null ) );
    }

    public <T> T makeBlockingRequest( AdvertisedSocketAddress catchUpAddress, CatchUpRequest request, CatchUpResponseCallback<T> responseHandler )
            throws CatchUpClientException
    {
        CompletableFuture<T> future = new CompletableFuture<>();

        CatchUpChannel channel = pool.acquire( catchUpAddress );

        future.whenComplete( ( result, e ) ->
        {
            if ( e == null )
            {
                pool.release( channel );
            }
            else
            {
                pool.dispose( channel );
            }
        } );

        channel.setResponseHandler( responseHandler, future );
        channel.send( request );

        String operation = String.format( "Timed out executing operation %s on %s (%s)", request, catchUpAddress.getHostname(), catchUpAddress.getPort() );

        return waitForCompletion( future, operation, channel::millisSinceLastResponse, inactivityTimeoutMillis, log );
    }

    private class CatchUpChannel implements CatchUpChannelPool.Channel
    {
        private final TrackingResponseHandler handler;
        private final AdvertisedSocketAddress destination;
        private Channel nettyChannel;

        CatchUpChannel( AdvertisedSocketAddress destination, CatchUpResponseCallback catchUpResponseCallback )
        {
            this.destination = destination;
            handler = new TrackingResponseHandler( catchUpResponseCallback, clock );
            Bootstrap bootstrap = new Bootstrap().group( eventLoopGroup ).channel( NioSocketChannel.class ).remoteAddress( destination.getHostname(),
                    destination.getPort() ).handler( new ChannelInitializer<SocketChannel>()
            {
                @Override
                protected void initChannel( SocketChannel ch ) throws Exception
                {
                    CatchUpClientChannelPipeline.initChannel( ch, handler, logProvider, monitors, sslPolicy );
                }
            } );

            ChannelFuture channelFuture = bootstrap.connect( destination.socketAddress() );
            nettyChannel = channelFuture.awaitUninterruptibly().channel();
        }

        void setResponseHandler( CatchUpResponseCallback responseHandler, CompletableFuture<?> requestOutcomeSignal )
        {
            handler.setResponseHandler( responseHandler, requestOutcomeSignal );
        }

        void send( CatchUpRequest request )
        {
            nettyChannel.write( request.messageType() );
            nettyChannel.writeAndFlush( request );
        }

        Optional<Long> millisSinceLastResponse()
        {
            return handler.lastResponseTime().map( lastResponseMillis -> clock.millis() - lastResponseMillis );
        }

        @Override
        public AdvertisedSocketAddress destination()
        {
            return destination;
        }

        @Override
        public void close()
        {
            nettyChannel.close();
        }
    }

    @Override
    public void start()
    {
        eventLoopGroup = new NioEventLoopGroup( 0, new NamedThreadFactory( "catch-up-client" ) );
    }

    @Override
    public void stop()
    {
        log.info( "CatchUpClient stopping" );
        try
        {
            pool.close();
            eventLoopGroup.shutdownGracefully( 0, 0, MICROSECONDS ).sync();
        }
        catch ( InterruptedException e )
        {
            log.warn( "Interrupted while stopping catch up client." );
        }
    }
}
