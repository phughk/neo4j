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

import org.neo4j.causalclustering.CatchupServerBuilder;
import org.neo4j.kernel.lifecycle.LifeSupport;

public class RemoteStoreIT
{
    private LifeSupport lifeSupport;

    CatchupServerBuilder catchUpServerBuilderOne;
    CatchupServerBuilder catchupServerBuilderTwo;
    RemoteStore subject;

    @Before
    public void setup()
    {
        lifeSupport = new LifeSupport();

        RemoteStoreBuilder remoteStoreBuilder = new RemoteStoreBuilder();
        catchUpServerBuilderOne = new CatchupServerBuilder();
        catchupServerBuilderTwo = new CatchupServerBuilder();
        subject = remoteStoreBuilder.getRemoteStore();
    }

    @Test
    public void canPerformCatchup()
    {
        // given remote node has a store


        // when catchup is performed for valid transactionId and StoreId
        subject.copy();

        // then the catchup is successful
    }

    @Test
    public void reconnectingWorks()
    {
        // given a remote catchup will fail midway

        // when a remote catchup is started

        // then the catchup is possible to complete
    }

    @Test
    public void catchupToDifferentStoreIdNotPossible()
    {
        // given a remote catchup will fail midway

        // and another node has a different
    }

    @Test
    public void catchupToDifferentTransactionIdNotPossible()
    {
        // given a remote catchup will fail midway

        // when catchup is performed

        // then catchup will fail because it shouldn't be possible to catchup from a node with a different transactionId
    }
}
