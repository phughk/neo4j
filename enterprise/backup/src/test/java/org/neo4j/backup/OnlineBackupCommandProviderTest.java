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
package org.neo4j.backup;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.helpers.Service;

public class OnlineBackupCommandProviderTest // TODO delete me
{
//    OnlineBackupCommandProvider subject;
//
//    @Before
//    public void setup()
//    {
//        subject = new OnlineBackupCommandProvider();
//    }

    private static List<BackupSupportingClassesFactoryProvider> findFactoryProviders()
    {
        List<BackupSupportingClassesFactoryProvider> providers = new ArrayList<>(  );
        Service.load( BackupSupportingClassesFactoryProvider.class ).spliterator().forEachRemaining( providers::add );
        return providers;
    }

    @Test
    public void providerLoadsCommunityFactory()
    {
        Assert.assertEquals( 1, findFactoryProviders().size() );
    }
}
