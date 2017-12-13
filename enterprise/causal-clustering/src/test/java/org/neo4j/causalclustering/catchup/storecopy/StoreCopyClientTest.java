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

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchUpClientException;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StoreCopyClientTest
{
    private final CatchUpClient catchUpClient = mock( CatchUpClient.class );

    StoreCopyClient subject;

    // params
    private final AdvertisedSocketAddress advertisedSocketAddress = new AdvertisedSocketAddress( "host", 1234 );
    private final StoreId storeId = new StoreId( 1, 2, 3, 4 );
    private final StoreFileStreams storeFileStreams = mock( StoreFileStreams.class );

    @Before
    public void setup()
    {
        subject = new StoreCopyClient( catchUpClient, NullLogProvider.getInstance() );
    }

    @Test
    public void storeCopyReturnsStateForResumability() throws StoreCopyFailedException, CatchUpClientException
    {
        // given a store copy fails mid way
        when( catchUpClient.makeBlockingRequest( advertisedSocketAddress, any(), any() ) ).thenThrow( CatchUpClientException.class );

        // when store copy is performed
        StoreCopyClient.StoreCopyResult result = subject.copyStoreFiles( advertisedSocketAddress, storeId, storeFileStreams );

        // then returned state reflects partial copy
        assertFalse( result.wasSuccessful );
    }
}
