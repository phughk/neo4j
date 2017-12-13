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

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class StoreListingResponseMarshalTest
{
    EmbeddedChannel embeddedChannel;

    @Before
    public void setup()
    {
        embeddedChannel = new EmbeddedChannel( new StoreListingResponseEncoder(), new StoreListingResponseDecoder() );
    }

    @Test
    public void transactionIdGetsTransmitted()
    {
        // given
        long transactionId = Long.MAX_VALUE;

        // when a transaction id is serialised
        StoreListingResponse storeListingResponse = new StoreListingResponse( new File[0], transactionId, new byte[0] );
        sendToChannel( storeListingResponse, embeddedChannel );

        // then it can be deserialised
        StoreListingResponse readStoreListingResponse = embeddedChannel.readInbound();
        assertEquals( storeListingResponse.transactionId, readStoreListingResponse.transactionId );
    }

    @Test
    public void fileListGetsTransmitted()
    {
        // given
        File[] files =
                new File[]{new File( "File a.txt" ), new File( "file-b" ), new File( "aoifnoasndfosidfoisndfoisnodainfsonidfaosiidfna" ), new File( "" )};

        // when
        StoreListingResponse storeListingResponse = new StoreListingResponse( files, 0L, new byte[0] );
        sendToChannel( storeListingResponse, embeddedChannel );

        // then it can be deserialised
        StoreListingResponse readStoreListingResponse = embeddedChannel.readInbound();
        assertEquals( storeListingResponse.files.length, readStoreListingResponse.files.length );
        for ( File file : files )
        {
            assertEquals( 1, Stream.of( readStoreListingResponse.files ).map( File::getName ).filter( f -> f.equals( file.getName() ) ).count() );
        }
    }

    @Test
    public void countContentsGetsTransmitted()
    {
        // given
        byte[] contents = "This is a big file".getBytes();

        // when
        StoreListingResponse storeListingResponse = new StoreListingResponse( new File[0], 0L, contents );
        sendToChannel( storeListingResponse, embeddedChannel );

        // then
        StoreListingResponse readStoreListingResponse = embeddedChannel.readInbound();
        assertArrayEquals( contents, readStoreListingResponse.countStoreContents );
    }

    private static void sendToChannel( StoreListingResponse storeListingResponse, EmbeddedChannel embeddedChannel )
    {
        embeddedChannel.writeOutbound( storeListingResponse );

        ByteBuf object = embeddedChannel.readOutbound();
        embeddedChannel.writeInbound( object );
    }
}
