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

import org.neo4j.causalclustering.catchup.CatchUpClientException;
import org.neo4j.causalclustering.catchup.HughCatchUpClient;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdRequest;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreRequest;
import org.neo4j.causalclustering.catchup.storecopy.StoreFileStreams;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class HughStoreCopyClient
{
    private final HughCatchUpClient catchUpClient;
    private final Log log;

    public HughStoreCopyClient( HughCatchUpClient catchUpClient, LogProvider logProvider )
    {
        this.catchUpClient = catchUpClient;
        log = logProvider.getLog( getClass() );
    }

    long copyStoreFiles( AdvertisedSocketAddress from, StoreId expectedStoreId, StoreFileStreams storeFileStreams ) throws StoreCopyFailedException
    {
        try
        {
            return catchUpClient.makeBlockingRequest( from, new GetStoreRequest( expectedStoreId ),
                    new CopyStoreFilesResponseAdaptor( storeFileStreams, log ) );
        }
        catch ( CatchUpClientException e )
        {
            throw new StoreCopyFailedException( e );
        }
    }

    StoreId fetchStoreId( AdvertisedSocketAddress fromAddress )
    {
        try
        {
            return catchUpClient.makeBlockingRequest( fromAddress, new GetStoreIdRequest(), new GetStoreIdResponseAdaptor() );
        }
        catch ( CatchUpClientException e )
        {
            throw new StoreIdDownloadFailedException( e );
        }
    }
}
