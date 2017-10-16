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

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.HttpConnector;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static java.lang.String.format;
import static org.neo4j.backup.BackupProtocolService.startTemporaryDb;

public class BackupRecoveryService
{
    private static final int MIN_PORT = 15000;
    private static final int MAX_PORT = 40000;

    private final Iterator<Integer> availablePortStream;

    public BackupRecoveryService()
    {
        availablePortStream = createAvailablePortStream();
    }

    public void recoverWithDatabase( File targetDirectory, PageCache pageCache, Config config )
    {
        Map<String,String> configParams = allocateRandomAvailablePorts( config );
        GraphDatabaseAPI targetDb = startTemporaryDb( targetDirectory, pageCache, configParams );
        targetDb.shutdown();
    }

    Map<String,String> allocateRandomAvailablePorts( Config config )
    {
        Map<String,String> configParams = config.getRaw();
        configParams.put( new BoltConnector().listen_address.name(), ":" + findRandomFreePort() );
        return configParams;
    }

    private int findRandomFreePort()
    {
        try
        {
            return availablePortStream.next();
        }
        catch ( NoSuchElementException e )
        {
            throw new IllegalStateException( format( "No more available ports between %d and %d", MIN_PORT, MAX_PORT ) );
        }
    }

    private static Iterator<Integer> createAvailablePortStream()
    {
        return IntStream
                .range( MIN_PORT, MAX_PORT )
                .filter( BackupRecoveryService::isPortAvailable )
                .iterator();
    }

    private static boolean isPortAvailable( int port )
    {
        try
        {
            new ServerSocket( port ).close();
            return true;
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            return false;
        }
    }
}
