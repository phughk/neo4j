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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.commons.compress.utils.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.catchup.CatchUpResponseHandler;
import org.neo4j.graphdb.Resource;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.storageengine.api.StoreFileMetadata;

public class StoreListingRequestHandler extends SimpleChannelInboundHandler<StoreListingRequest>
{
    private final Supplier<CheckPointer> checkPointerSupplier;
    private final StoreCopyCheckPointMutex mutex;
    private Supplier<NeoStoreDataSource> dataSourceSupplier;

    public StoreListingRequestHandler( Supplier<CheckPointer> checkPointerSupplier, StoreCopyCheckPointMutex mutex,
            Supplier<NeoStoreDataSource> dataSourceSupplier )
    {
        this.checkPointerSupplier = checkPointerSupplier;
        this.mutex = mutex;
        this.dataSourceSupplier = dataSourceSupplier;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext channelHandlerContext, StoreListingRequest storeListingRequest ) throws Exception
    {
        // TODO handle storeId mismatch
//        if ( !msg.expectedStoreId().equalToKernelStoreId( dataSource.get().getStoreId() ) )
//        {
//            channelHandlerContext.fail( ctx, E_STORE_ID_MISMATCH );
//        }
//        else {

        CheckPointer checkPointer = checkPointerSupplier.get();
        try ( Resource checkPointLock = acquireMutex( checkPointer ) )
        {

            StoreListingResponse storeListingResponse = constructStoreListingResponse();
            releaseMutex( checkPointLock );

            channelHandlerContext.writeAndFlush( storeListingResponse );
        }
    }

    private StoreListingResponse constructStoreListingResponse() throws IOException
    {
        NeoStoreDataSource dataSource = dataSourceSupplier.get();

        return new StoreListingResponse( getFiles( dataSource ), lastTransactionId(), readCountStore( dataSource ) );
    }

    private Resource acquireMutex( CheckPointer checkPointer ) throws IOException
    {
        return mutex.storeCopy( () -> checkPointer.tryCheckPoint( new SimpleTriggerInfo( "Store copy" ) ) );
    }

    private void releaseMutex( Resource checkPointLock )
    {
        checkPointLock.close();
    }

    private long lastTransactionId()
    {
        return 5;
    }

    private File[] getFiles( NeoStoreDataSource dataSource ) throws IOException
    {
        File storeDir = dataSource.getStoreDir();
        List<StoreFileMetadata> files = files( dataSource ).filter( isCountFile().negate() ).collect( Collectors.toList() );
        List<File> aggregated = new ArrayList<>();
        for ( StoreFileMetadata storeFileMetadata : files )
        {
            aggregated.add( new File( FileUtils.relativePath( storeDir, storeFileMetadata.file() ) ) );// TODO not file
        }
        File[] arr = new File[aggregated.size()];
        aggregated.toArray( arr );
        return arr;
    }

    private static Predicate<StoreFileMetadata> isCountFile()
    {
        return storeFileMetadata -> StoreType.typeOf( storeFileMetadata.file().getName() ).filter( f -> f == StoreType.COUNTS ).isPresent();
    }

    private static Stream<StoreFileMetadata> files( NeoStoreDataSource dataSource ) throws IOException
    {
        return dataSource.listStoreFiles( false ).stream();
    }

    private static byte[] readCountStore( NeoStoreDataSource dataSource ) throws IOException
    {
        List<StoreFileMetadata> files = files( dataSource ).filter( isCountFile() ).collect( Collectors.toList() );
        if ( files.size() != 1 )
        {
            throw new RuntimeException( String.format( "There was not 1 file, there were %d: %s", files.size(), files ) );
        }
        StoreFileMetadata countStore = files.get( 0 );
        return readFile( countStore.file() );
    }

    private static byte[] readFile( File f ) throws IOException
    {
        FileInputStream fileInputStream = new FileInputStream( f );
        return IOUtils.toByteArray( fileInputStream );
    }
}
