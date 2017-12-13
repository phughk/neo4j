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

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.core.state.storage.SafeChannelMarshal;
import org.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class StoreListingResponse
{
    public final File[] files;
    public final Long transactionId;
    public byte[] countStoreContents;

    public StoreListingResponse( File[] files, Long transactionId, byte[] countStoreContents )
    {
        this.files = files;
        this.transactionId = transactionId;
        this.countStoreContents = countStoreContents;
    }

    public static class StoreListingMarshal extends SafeChannelMarshal<StoreListingResponse>
    {
        @Override
        public void marshal( StoreListingResponse storeListingResponse, WritableChannel buffer ) throws IOException
        {
            buffer.putLong( storeListingResponse.transactionId );
            marshalFiles( buffer, storeListingResponse.files );
            marshalCountStore( storeListingResponse, buffer );
        }

        @Override
        protected StoreListingResponse unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
        {
            Long transactionId = channel.getLong();
            File[] files = unmarshalFiles( channel );
            byte[] countStoreContents = unmarshalCountStoreContents( channel );
            return new StoreListingResponse( files, transactionId, countStoreContents );
        }

        private static void marshalCountStore( StoreListingResponse storeListingResponse, WritableChannel buffer ) throws IOException
        {
            byte[] bytes = storeListingResponse.countStoreContents;
            buffer.putInt( bytes.length );
            buffer.put( bytes, bytes.length );
        }

        private static byte[] unmarshalCountStoreContents( ReadableChannel channel ) throws IOException
        {
            int size = channel.getInt();
            byte[] content = new byte[size];
            channel.get( content, size );
            return content;
        }

        private static void marshalFiles( WritableChannel buffer, File[] files ) throws IOException
        {
            buffer.putInt( files.length );
            for ( File file : files )
            {
                byte[] bytes = file.getName().getBytes(); // TODO consider if relative or flat
                buffer.putInt( bytes.length );
                buffer.put( bytes, bytes.length );
            }
        }

        private static File[] unmarshalFiles( ReadableChannel channel ) throws IOException
        {
            int numberOfFiles = channel.getInt();
            File[] files = new File[numberOfFiles];
            for ( int i = 0; i < numberOfFiles; i++ )
            {
                files[i] = unmarshalFile( channel );
            }
            return files;
        }

        private static File unmarshalFile( ReadableChannel channel ) throws IOException
        {
            int characters = channel.getInt();
            byte[] name = new byte[characters];
            channel.get( name, characters );
            return new File( new String( name ) );
        }
    }
}
