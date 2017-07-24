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
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.neo4j.backup.nextgen.BackupDelegator;
import org.neo4j.backup.nextgen.BackupModule;
import org.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.commandline.arguments.MandatoryNamedArg;
import org.neo4j.commandline.arguments.OptionalBooleanArg;
import org.neo4j.commandline.arguments.OptionalNamedArg;
import org.neo4j.commandline.arguments.common.MandatoryCanonicalPath;
import org.neo4j.commandline.arguments.common.OptionalCanonicalPath;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.CheckConsistencyConfig;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;

public class OnlineBackupCommand implements AdminCommand
{

    private static final Arguments arguments = new Arguments()
            .withArgument( new MandatoryCanonicalPath( "backup-dir", "backup-path",
                    "Directory to place backup in." ) )
            .withArgument( new MandatoryNamedArg( "name", "graph.db-backup",
                    "Name of backup. If a backup with this name already exists an incremental backup will be " +
                            "attempted." ) )
            .withArgument( new OptionalNamedArg( "from", "address", "localhost:6362",
                    "Host and port of Neo4j." ) )
            .withArgument( new OptionalBooleanArg( "fallback-to-full", true,
                    "If an incremental backup fails backup will move the old backup to <name>.err.<N> and " +
                            "fallback to a full backup instead." ) )
            .withArgument( new OptionalNamedArg( "timeout", "timeout", "20m",
                    "Timeout in the form <time>[ms|s|m|h], where the default unit is seconds." ) )
            .withArgument( new OptionalBooleanArg( "check-consistency", true,
                    "If a consistency check should be made." ) )
            .withArgument( new OptionalCanonicalPath( "cc-report-dir", "directory", ".",
                    "Directory where consistency report will be written." ) )
            .withArgument( new OptionalCanonicalPath( "additional-config", "config-file-path", "",
                    "Configuration file to supply additional configuration in. This argument is DEPRECATED." ) )
            .withArgument( new OptionalBooleanArg( "cc-graph", true,
                    "Perform consistency checks between nodes, relationships, properties, types and tokens." ) )
            .withArgument( new OptionalBooleanArg( "cc-indexes", true,
                    "Perform consistency checks on indexes." ) )
            .withArgument( new OptionalBooleanArg( "cc-label-scan-store", true,
                    "Perform consistency checks on the label scan store." ) )
            .withArgument( new OptionalBooleanArg( "cc-property-owners", false,
                    "Perform additional consistency checks on property ownership. This check is *very* expensive in " +
                            "time and memory." ) );

    public static Arguments arguments()
    {
        return arguments;
    }

    static final int STATUS_CC_ERROR = 2;
    static final int STATUS_CC_INCONSISTENT = 3;
    static final int MAX_OLD_BACKUPS = 1000;
    private final BackupService backupService;
    private ConsistencyCheckService consistencyCheckService;
    private final OutsideWorld outsideWorld;
    private final LogProvider logProvider;
    private final CommandFailedSupplier<BackupModule> backupModuleSupplier;
    private final OnlineBackupCommandConfigLoader onlineBackupCommandConfigLoader;

    public OnlineBackupCommand( BackupService backupService, ConsistencyCheckService consistencyCheckService, OutsideWorld outsideWorld,
            CommandFailedSupplier<BackupModule> backupModuleSupplier, OnlineBackupCommandConfigLoader onlineBackupCommandConfigLoader )
    {
        this.backupService = backupService;
        this.consistencyCheckService = consistencyCheckService;
        this.outsideWorld = outsideWorld;
        this.backupModuleSupplier = backupModuleSupplier;
        this.onlineBackupCommandConfigLoader = onlineBackupCommandConfigLoader;

        this.logProvider = FormattedLogProvider.toOutputStream( outsideWorld.outStream() );
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        BackupModule backupModule = backupModuleSupplier.get();
        final OnlineBackupRequiredArguments requiredArgs = backupModule.establishRequiredArguments( args, arguments );
        final OnlineBackupConfigurationOverride configurationOverride;

        // Make sure destination exists
        checkDestination( requiredArgs.folder );
        checkDestination( requiredArgs.reportDir );

        File destination = requiredArgs.folder.resolve( requiredArgs.name ).toFile();
        Config config = onlineBackupCommandConfigLoader.loadConfig( requiredArgs.additionalConfig ).get();

        configurationOverride = backupModule.establishConfigurationOverride( config, arguments );
        final BackupDelegator backupDelegator = backupModule.backupDelegator( destination, requiredArgs.addressAsAdvertised() );

        boolean done = false;
        if ( backupExists( outsideWorld, destination ) )
        {
            done = performIncrementalBackup( config, requiredArgs, destination, backupDelegator );
        }

        if ( !done )
        {
            performFullBackup( config, requiredArgs, destination );
        }

        if ( requiredArgs.doConsistencyCheck )
        {
            performConsistencyCheck( config, requiredArgs, configurationOverride, destination );
        }

        outsideWorld.stdOutLine( "Backup complete." );
    }

    private void checkDestination( Path path ) throws CommandFailed
    {
        if ( !outsideWorld.fileSystem().isDirectory( path.toFile() ) )
        {
            throw new CommandFailed( String.format( "Directory '%s' does not exist.", path ) );
        }
    }

    private void performConsistencyCheck( Config config, OnlineBackupRequiredArguments requiredArgs, OnlineBackupConfigurationOverride configurationOverride,
            File destination ) throws CommandFailed
    {
        try
        {
            outsideWorld.stdOutLine( "Doing consistency check..." );
            ConsistencyCheckService.Result ccResult =
                    consistencyCheckService.runFullConsistencyCheck( destination, config, ProgressMonitorFactory.textual( outsideWorld.errorStream() ),
                            logProvider, outsideWorld.fileSystem(), false, requiredArgs.reportDir.toFile(),
                            new CheckConsistencyConfig( configurationOverride.checkGraph, configurationOverride.checkIndexes,
                                    configurationOverride.checkLabelScanStore, configurationOverride.checkPropertyOwners ) );

            if ( !ccResult.isSuccessful() )
            {
                throw new CommandFailed( String.format( "Inconsistencies found. See '%s' for details.", ccResult.reportFile() ), STATUS_CC_INCONSISTENT );
            }
        }
        catch ( Throwable e )
        {
            if ( e instanceof CommandFailed )
            {
                throw (CommandFailed) e;
            }
            throw new CommandFailed( "Failed to do consistency check on backup: " + e.getMessage(), e, STATUS_CC_ERROR );
        }
    }

    private boolean performIncrementalBackup( Config config, OnlineBackupRequiredArguments requiredArgs, File destination, BackupDelegator backupDelegator )
            throws CommandFailed
    {
//        try
//        {
//            return tryCausalClusteringIncrementalBackup( config, requiredArgs, destination, backupDelegator );
//        }
//        catch ( StoreIdDownloadFailedException e )
//        {
//            outsideWorld.stdErrLine( "Unable to retrieve store id from specified cluster node (RAFT catch up aborted): " + e.getMessage() );
            return tryHighAvailabilityIncrementalBackup( config, requiredArgs, destination );
//        }
    }

    private boolean tryCausalClusteringIncrementalBackup( Config config /*TODO this needs to be used*/, OnlineBackupRequiredArguments requiredArgs,
            File destination, BackupDelegator backupDelegator ) throws StoreIdDownloadFailedException
    {
        AdvertisedSocketAddress fromAddress = new AdvertisedSocketAddress( requiredArgs.address.getHost(), requiredArgs.address.getPort() );
        StoreId expectedStoreId = backupDelegator.fetchStoreId( fromAddress );
        backupDelegator.tryCatchingUp( fromAddress, expectedStoreId, destination );
        return true;
    }

    private boolean tryHighAvailabilityIncrementalBackup( Config config, OnlineBackupRequiredArguments requiredArgs, File destination ) throws CommandFailed
    {
        outsideWorld.stdOutLine( "Destination is not empty, doing incremental backup..." );
        try
        {
            backupService.doIncrementalBackup( requiredArgs.address.getHost(), requiredArgs.address.getPort(), destination, ConsistencyCheck.NONE,
                    requiredArgs.timeout, config );
            return true;
        }
        catch ( Exception e )
        {
            if ( requiredArgs.fallbackToFull )
            {
                outsideWorld.stdErrLine( "Incremental backup failed: " + e.getMessage() );
                String renamed = renameExistingBackup( requiredArgs.folder, requiredArgs.name );
                outsideWorld.stdErrLine( String.format( "Old backup renamed to '%s'.", renamed ) );
                return false;
            }
            else
            {
                throw new CommandFailed( "Backup failed: " + e.getMessage(), e );
            }
        }
    }

    private void performFullBackup( Config config, OnlineBackupRequiredArguments requiredArgs, File destination ) throws CommandFailed
    {
        outsideWorld.stdOutLine( "Doing full backup..." );
        try
        {
            backupService.doFullBackup( requiredArgs.address.getHost(), requiredArgs.address.getPort(), destination, ConsistencyCheck.NONE, config,
                    requiredArgs.timeout, false );
        }
        catch ( Exception e )
        {
            throw new CommandFailed( "Backup failed: " + e.getMessage(), e );
        }
    }

    private String renameExistingBackup( final Path folder, final String oldName ) throws CommandFailed
    {
        int i = 1;
        while ( i < MAX_OLD_BACKUPS )
        {
            String newName = oldName + ".err." + i;
            if ( outsideWorld.fileSystem().fileExists( folder.resolve( newName ).toFile() ) )
            {
                i++;
            }
            else
            {
                try
                {
                    outsideWorld.fileSystem().renameFile( folder.resolve( oldName ).toFile(), folder.resolve( newName ).toFile() );
                    return newName;
                }
                catch ( IOException e )
                {
                    throw new CommandFailed( "Failed to move old backup out of the way: " + e.getMessage(), e );
                }
            }
        }
        throw new CommandFailed( "Failed to move old backup out of the way: too many old backups." );
    }

    private long parseTimeout( String[] args )
    {
        return Args.parse( args ).getDuration( "timeout", TimeUnit.MINUTES.toMillis( 20 ) );
    }

    private static boolean backupExists( OutsideWorld outsideWorld, File destination )
    {
        File[] listFiles = outsideWorld.fileSystem().listFiles( destination );
        return listFiles != null && listFiles.length > 0;
    }
}
