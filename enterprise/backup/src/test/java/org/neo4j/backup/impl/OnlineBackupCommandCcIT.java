/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.backup.impl;

import org.apache.commons.lang3.SystemUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.ClusterHelper;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.helpers.CausalClusteringTestHelpers;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.causalclustering.ClusterRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.util.TestHelpers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

@RunWith( Parameterized.class )
public class OnlineBackupCommandCcIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Rule
    public ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 3 )
            .withSharedCoreParam( CausalClusteringSettings.cluster_topology_refresh, "5s" );

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( SuppressOutput.suppressAll() ).around( clusterRule );

    private File backupDir;

    private List<Runnable> oneOffShutdownTasks;

    @Parameter
    public String recordFormat;

    @Parameters( name = "{0}" )
    public static List<String> recordFormats()
    {
        return Arrays.asList( Standard.LATEST_NAME, HighLimit.NAME );
    }

    @Before
    public void initialiseBackupDirectory()
    {
        oneOffShutdownTasks = new ArrayList<>();
        backupDir = testDirectory.directory( "backups" );
    }

    @After
    public void performShutdownTasks()
    {
        oneOffShutdownTasks.forEach( Runnable::run );
    }

    @Test
    public void backupCanBePerformedOverCcWithCustomPort() throws Exception
    {
        assumeFalse( SystemUtils.IS_OS_WINDOWS );

        Cluster cluster = startCluster( recordFormat );
        String customAddress = CausalClusteringTestHelpers.transactionAddress( clusterLeader( cluster ).database() );
        assertEquals(
                1,
                runBackupToolFromOtherJvmToGetExitCode( "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=defaultport" ) );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "--from", customAddress,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=defaultport" ) );
        assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( "defaultport", backupDir) );

        createSomeData( cluster );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "--from", customAddress,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=defaultport" ) );
        assertEquals( DbRepresentation.of( clusterDatabase( cluster ) ), getBackupDbRepresentation( "defaultport", backupDir ) );
    }

    @Test
    public void backupCanNotBePerformedOverBackupProtocol() throws Exception
    {
        assumeFalse( SystemUtils.IS_OS_WINDOWS );

        Cluster cluster = startCluster( recordFormat );
        String ip = TestHelpers.backupAddressHa( clusterLeader( cluster ).database() );
        assertEquals(
                1,
                runBackupToolFromOtherJvmToGetExitCode( "--from", ip,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=defaultport" ) );
    }

    @Test
    public void dataIsInAUsableStateAfterBackup() throws Exception
    {
        // given database exists
        Cluster cluster = startCluster( recordFormat );

        // and the database has indexes
        ClusterHelper.createIndexes( cluster.getDbWithAnyRole( Role.LEADER ).database() );

        // and the database is being populated
        populateDatabaseOnSeparateThread( cluster );

        // then backup is successful
        String address = TestHelpers.backupAddressCc( clusterLeader( cluster ).database() );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( "--from", address, "--cc-report-dir=" + backupDir, "--backup-dir=" + backupDir,
                "--name=defaultport" ) );
    }

    @Test
    public void transactionsAreNotKeptAfterBackup() throws Exception
    {
        // given
        Cluster cluster = startCluster( recordFormat );
        String name = "txLogFilesNotRetained";

        // and
        createSomeData( cluster );
        AtomicBoolean kill = populateDatabaseOnSeparateThread( cluster );

        // and full backup has been performed
        String address = TestHelpers.backupAddressCc( clusterLeader( cluster ).database() );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( "--from", address, "--name=" + name, "--backup-dir=" + backupDir ) );

        // and conditions for incremental backup exist
        createSomeData( cluster );

        // when incremental backup is performed
        Thread.sleep( 2000 );
        kill.set( false );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( "--from", address, "--name=" + name, "--backup-dir=" + backupDir ) );

        // then there are no tx logs
        File backupLocation = new File( backupDir, name );
        assertEquals( Collections.emptyList(), findTransactionLogFiles( backupLocation ) );
    }

    /**
     * Start a new thread and repeatedly populate the database
     * @param cluster cluster
     * @return kill switch, when false thread will stop running
     */
    private AtomicBoolean populateDatabaseOnSeparateThread( Cluster cluster )
    {
        AtomicBoolean populateDatabaseFlag = new AtomicBoolean( true );
        new Thread( () -> repeatedlyPopulateDatabase( cluster, populateDatabaseFlag ) ).start(); // populate db with number properties etc.
        oneOffShutdownTasks.add( () -> populateDatabaseFlag.set( false ) ); // kill thread
        return populateDatabaseFlag;
    }

    private static List<File> findTransactionLogFiles( File location )
    {
        List<File> listed = Arrays.asList( Optional.ofNullable( location.listFiles() ).orElse( new File[]{} ) );
        return listed.stream()
                .filter( isTransactionLogFile( location ) )
                .collect( Collectors.toList() );
    }

    private static Predicate<File> isTransactionLogFile( File location )
    {
        LogFiles logFiles;
        try
        {
            logFiles = LogFilesBuilder.builder( location, new DefaultFileSystemAbstraction())
                    .withLogVersionRepository( new SimpleLogVersionRepository() )
                    .withTransactionIdStore( new SimpleTransactionIdStore(  ) )
                    .build();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        return logFiles::isLogFile;
    }

    private void repeatedlyPopulateDatabase( Cluster cluster, AtomicBoolean continueFlagReference )
    {
        while ( continueFlagReference.get() )
        {
            createSomeData( cluster );
        }
    }

    public static CoreGraphDatabase clusterDatabase( Cluster cluster )
    {
        return clusterLeader( cluster ).database();
    }

    private Cluster startCluster( String recordFormat )
            throws Exception
    {
        ClusterRule clusterRule = this.clusterRule
                .withSharedCoreParam( GraphDatabaseSettings.record_format, recordFormat )
                .withSharedReadReplicaParam( GraphDatabaseSettings.record_format, recordFormat );
        Cluster cluster = clusterRule.startCluster();
        createSomeData(  cluster );
        return cluster;
    }

    public static DbRepresentation createSomeData( Cluster cluster )
    {
        try
        {
            cluster.coreTx( ClusterHelper::createSomeData );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
        return DbRepresentation.of( clusterLeader( cluster ).database() );
    }

    private static CoreClusterMember clusterLeader( Cluster cluster )
    {
        return cluster.getDbWithRole( Role.LEADER );
    }

    public static DbRepresentation getBackupDbRepresentation( String name, File backupDir )
    {
        Config config = Config.defaults();
        config.augment( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
        return DbRepresentation.of( new File( backupDir, name ), config );
    }

    private int runBackupToolFromOtherJvmToGetExitCode( String... args ) throws Exception
    {
        return TestHelpers.runBackupToolFromOtherJvmToGetExitCode( testDirectory.absolutePath(), args );
    }
}
