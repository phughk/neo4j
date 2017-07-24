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
package org.neo4j.backup.nextgen;

import java.io.File;

import org.neo4j.backup.OnlineBackupCommandConfigLoader;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;

public class Application implements Runnable
{
    private File storeDir;
    private FileSystemAbstraction fileSystemAbstraction;
    private AdvertisedSocketAddress fromAddress;
    private BackupDelegator backupDelegator;

    public Application( File storeDir, FileSystemAbstraction fs, AdvertisedSocketAddress fromAddress, BackupDelegator backupDelegator )
    {
        this.storeDir = storeDir;
        this.fileSystemAbstraction = fs;
        this.fromAddress = fromAddress;
        this.backupDelegator = backupDelegator;
    }

    public static void main( String[] args )
    {
        File file = new File( "deleteThis/" );
        AdvertisedSocketAddress fromAddress = new AdvertisedSocketAddress( "127.0.0.1", 6000 );
        BackupModule backupDependencies = null;
        try
        {
            backupDependencies = new BackupModule( Config::defaults, NullLogProvider.getInstance() );
        }
        catch ( CommandFailed commandFailed )
        {
            throw new RuntimeException( commandFailed );
        }
        FileSystemAbstraction fileSystemAbstraction = backupDependencies.fileSystemAbstraction();
        BackupDelegator backupDelegator = backupDependencies.backupDelegator( file, fromAddress );

        Application application = new Application( file, fileSystemAbstraction, fromAddress, backupDelegator );
        application.run();
    }

    @Override
    public void run()
    {
        start();
        try
        {
            StoreId expectedStoreId = null;
            try
            {
                expectedStoreId = backupDelegator.fetchStoreId( fromAddress );
            }
            catch ( org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException e )
            {
                throw new RuntimeException( e );
            }
            backupDelegator.retrieveStore( expectedStoreId );
            backupDelegator.tryCatchingUp( fromAddress, expectedStoreId, storeDir );
            backupDelegator.clearIdFiles( fileSystemAbstraction, storeDir );
        }
        finally
        {
            stop();
        }
        backupDelegator.fixLocalStorage( storeDir );
    }

    private void start()
    {
        backupDelegator.start();
    }

    private void stop()
    {
        backupDelegator.stop();
    }
}
