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
package org.neo4j.backup;

import org.apache.commons.lang3.SystemUtils;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.neo4j.com.ports.allocation.PortAuthority;
import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.AdminTool;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.admin.RealOutsideWorld;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.proc.ProcessUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

@RunWith( Parameterized.class )
public class OnlineBackupCommandIT
{
    @ClassRule
    public static final TestDirectory testDirectory = TestDirectory.testDirectory();

    private final EmbeddedDatabaseRule db = new EmbeddedDatabaseRule().startLazily();

    @Rule
//    public final RuleChain ruleChain = RuleChain.outerRule( SuppressOutput.suppressAll() ).around( db );
    public final RuleChain ruleChain = RuleChain.outerRule( db );

    private final File backupDir = testDirectory.directory( "backups" );

    @Parameter
    public String recordFormat;

    @Parameters( name = "{0}" )
    public static List<String> recordFormats()
    {
        return Arrays.asList( /*Standard.LATEST_NAME, */HighLimit.NAME );
    }

    public static DbRepresentation createSomeData( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( "name", "Neo" );
            db.createNode().createRelationshipTo( node, RelationshipType.withName( "KNOWS" ) );
            tx.success();
        }
        return DbRepresentation.of( db );
    }

    @Test
    public void makeSureBackupCanBePerformedWithCustomPort() throws Exception
    {
        assumeFalse( SystemUtils.IS_OS_WINDOWS );

        int backupPort = PortAuthority.allocatePort();
        startDb( backupPort );
        assertEquals( "should not be able to do backup when noone is listening",
                1,
                runBackupToolFromOtherJvmToGetExitCode( "--from", "127.0.0.1:" + PortAuthority.allocatePort(),
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=customport" ) );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "--from", "127.0.0.1:" + backupPort,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=customport" ) );
        assertEquals( getDbRepresentation(), getBackupDbRepresentation( "customport" ) );
        createSomeData( db );
        assertEquals(
                0,
                runBackupToolFromOtherJvmToGetExitCode( "--from", "127.0.0.1:" + backupPort,
                        "--cc-report-dir=" + backupDir,
                        "--backup-dir=" + backupDir,
                        "--name=customport" ) );
        assertEquals( getDbRepresentation(), getBackupDbRepresentation( "customport" ) );
    }

    @Test
    public void transactionFilesGetDeleted()
    {
        // given we have expected transaction file names
        String expectedTxLog1 = "neostore.transaction.db.0";
        String expectedTxLog2 = "neostore.transaction.db.1";
        File serverLocation = db.getStoreDir();
        File serverTxLog1 = new File( serverLocation, expectedTxLog1 );
        File serverTxLog2 = new File( serverLocation, expectedTxLog2 );

        // and we have a instance set up in such a way that we have one tx log file
//        db.setConfig( GraphDatabaseSettings.keep_logical_logs, "1k txs" ); // (not?) required
        db.setConfig( GraphDatabaseSettings.logical_log_rotation_threshold, "1M" ); // Minimum is 1M
        db.setConfig( GraphDatabaseSettings.check_point_interval_tx, "5" );
        int backupPort = PortAuthority.allocatePort();
        startDb( backupPort );
        assertTrue( serverTxLog1.toString(), serverTxLog1.exists() );
        assertFalse( serverTxLog2.toString(), serverTxLog2.exists() );

        // and we have a full backup with transactions
        File backupLocation = testDirectory.absolutePath();
        String backupName = "myBackup_" + recordFormat;

        // when full backup
        File backupTxLog1 = Paths.get( backupLocation.toString(), "backups", backupName, expectedTxLog1 ).toFile();
        File backupTxLog2 = Paths.get( backupLocation.toString(), "backups", backupName, expectedTxLog2 ).toFile();
        assertEquals( 0,
                runBackupToolFromSameJvmToGetExistCode( backupLocation.toPath(), "--from", "127.0.0.1:" + backupPort, "--backup-dir", backupDir.toString(),
                        "--name", backupName ) );
        assertTrue( backupTxLog1.toString(), backupTxLog1.exists() );
        assertFalse( backupTxLog2.toString(), backupTxLog2.exists() );

        // and we have more data for a second transaction file to be created
        createTransactions( 3001 );
        assertTrue( serverTxLog1.toString(), serverTxLog1.exists() );
        assertTrue( serverTxLog2.toString(), serverTxLog2.exists() );

        // and performing incremental backup removes previous tx log file
        assertEquals( 0,
                runBackupToolFromSameJvmToGetExistCode( backupLocation.toPath(), "--from", "127.0.0.1:" + backupPort, "--backup-dir", backupDir.toString(),
                        "--name", backupName ) );
        assertFalse( backupTxLog1.toString(), backupTxLog1.exists() );
        assertTrue( backupTxLog2.toString(), backupTxLog2.exists() );
    }

    private void createTransactions( int numberOfTransactions )
    {
        System.out.printf( "[%s] Started\n", LocalDateTime.now().toLocalTime() );
        int tenPercent = numberOfTransactions / 10;
        LocalDateTime previous = LocalDateTime.now();
        for ( int i = 0; i < numberOfTransactions; i++ )
        {
            if ( i % tenPercent == 0 )
            {
                LocalDateTime current = LocalDateTime.now();
                long duration = Duration.between( previous, current).toMillis() / 1000;
                System.out.printf( "[%s](+%d) %d of %d tx complete\n", current.toLocalTime(), duration, i, numberOfTransactions );
                previous = current;
            }
            createSomeData( db );
        }
        System.out.printf( "[%s] Ended\n", LocalDateTime.now().toLocalTime() );
    }

    private void startDb( int backupPort )
    {
        db.setConfig( GraphDatabaseSettings.record_format, recordFormat );
        db.setConfig( OnlineBackupSettings.online_backup_enabled, Settings.TRUE );
        db.setConfig( OnlineBackupSettings.online_backup_server, "127.0.0.1" + ":" + backupPort );
        db.ensureStarted();
        createSomeData( db );
    }

    private static int runBackupToolFromSameJvmToGetExistCode( Path homeDir, String... args )
    {
        Path configDir = new File( homeDir.toFile(), "config" ).toPath();
        OutsideWorld outsideWorld = new RealOutsideWorld();
        OnlineBackupCommandProvider onlineBackupCommandProvider = new OnlineBackupCommandProvider();
        AdminCommand onlineBackupCommand = onlineBackupCommandProvider.create( homeDir, configDir, outsideWorld );
        try
        {
            onlineBackupCommand.execute( args );
            return 0;
        }
        catch ( IncorrectUsage | CommandFailed incorrectUsage )
        {
            incorrectUsage.printStackTrace();
            return 1;
        }
    }

    private static int runBackupToolFromOtherJvmToGetExitCode( String... args )
            throws Exception
    {
        return runBackupToolFromOtherJvmToGetExitCode( testDirectory.absolutePath(), args );
    }

    public static int runBackupToolFromOtherJvmToGetExitCode( File neo4jHome, String... args )
            throws Exception
    {
        List<String> allArgs = new ArrayList<>( Arrays.asList(
                ProcessUtil.getJavaExecutable().toString(), "-cp", ProcessUtil.getClassPath(),
                AdminTool.class.getName() ) );
        allArgs.add( "backup" );
        allArgs.addAll( Arrays.asList( args ) );

        Process process = Runtime.getRuntime().exec( allArgs.toArray( new String[allArgs.size()] ),
                new String[] {"NEO4J_HOME=" + neo4jHome.getAbsolutePath()} );
        return new ProcessStreamHandler( process, true ).waitForResult();
    }

    private DbRepresentation getDbRepresentation()
    {
        return DbRepresentation.of( db );
    }

    private DbRepresentation getBackupDbRepresentation( String name )
    {
        Config config = Config.defaults( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );

        return DbRepresentation.of( new File( backupDir, name ), config );
    }
}
