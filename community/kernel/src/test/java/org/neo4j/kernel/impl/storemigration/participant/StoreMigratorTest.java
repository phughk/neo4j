/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.storemigration.participant;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_2;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.store.MetaDataStore.DEFAULT_NAME;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_CHECKSUM;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_COMMIT_TIMESTAMP;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_ID;
import static org.neo4j.kernel.impl.store.MetaDataStore.getRecord;
import static org.neo4j.kernel.impl.store.MetaDataStore.setRecord;
import static org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat.FIELD_NOT_PRESENT;

public class StoreMigratorTest
{
    private final TestDirectory directory = TestDirectory.testDirectory();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final RandomRule random = new RandomRule();
    private final SchemaIndexProvider schemaIndexProvider = new InMemoryIndexProvider();
    private PageCache pageCache;

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( directory )
            .around( fileSystemRule )
            .around( pageCacheRule )
            .around( random );

    @Before
    public void setUp()
    {
        pageCache = pageCacheRule.getPageCache( fileSystemRule );
    }

    @Test
    public void shouldExtractTransactionInformationFromMetaDataStore() throws Exception
    {
        // given
        // ... variables
        long txId = 42;
        long checksum = 123456789123456789L;
        long timestamp = 919191919191919191L;
        TransactionId expected = new TransactionId( txId, checksum, timestamp );

        // ... and files
        File storeDir = directory.graphDbDir();
        File neoStore = new File( storeDir, DEFAULT_NAME );
        neoStore.createNewFile();

        // ... and mocks
        Config config = mock( Config.class );
        LogService logService = mock( LogService.class );

        // when
        // ... data in record
        setRecord( pageCache, neoStore, LAST_TRANSACTION_ID, txId );
        setRecord( pageCache, neoStore, LAST_TRANSACTION_CHECKSUM, checksum );
        setRecord( pageCache, neoStore, LAST_TRANSACTION_COMMIT_TIMESTAMP, timestamp );

        // ... and with migrator
        StoreMigrator migrator = new StoreMigrator( fileSystemRule.get(), pageCache, config, logService );
        TransactionId actual = migrator.extractTransactionIdInformation( neoStore, storeDir, txId );

        // then
        assertEquals( expected, actual );
    }

    @Test
    public void shouldGenerateTransactionInformationWhenLogsNotPresent() throws Exception
    {
        // given
        long txId = 42;
        File storeDir = directory.graphDbDir();
        File neoStore = new File( storeDir, DEFAULT_NAME );
        neoStore.createNewFile();
        Config config = mock( Config.class );
        LogService logService = new SimpleLogService( NullLogProvider.getInstance(), NullLogProvider.getInstance() );

        // when
        // ... transaction info not in neo store
        assertEquals( FIELD_NOT_PRESENT, getRecord( pageCache, neoStore, LAST_TRANSACTION_ID ) );
        assertEquals( FIELD_NOT_PRESENT, getRecord( pageCache, neoStore, LAST_TRANSACTION_CHECKSUM ) );
        assertEquals( FIELD_NOT_PRESENT, getRecord( pageCache, neoStore, LAST_TRANSACTION_COMMIT_TIMESTAMP ) );
        // ... and with migrator
        StoreMigrator migrator = new StoreMigrator( fileSystemRule.get(), pageCache, config, logService );
        TransactionId actual = migrator.extractTransactionIdInformation( neoStore, storeDir, txId );

        // then
        assertEquals( txId, actual.transactionId() );
        assertEquals( TransactionIdStore.UNKNOWN_TX_CHECKSUM, actual.checksum() );
        assertEquals( TransactionIdStore.UNKNOWN_TX_COMMIT_TIMESTAMP, actual.commitTimestamp() );
    }

    @Test
    public void shouldGenerateTransactionInformationWhenLogsAreEmpty() throws Exception
    {
        // given
        long txId = 1;
        File storeDir = directory.graphDbDir();
        File neoStore = new File( storeDir, DEFAULT_NAME );
        neoStore.createNewFile();
        Config config = mock( Config.class );
        LogService logService = new SimpleLogService( NullLogProvider.getInstance(), NullLogProvider.getInstance() );

        // when
        // ... transaction info not in neo store
        assertEquals( FIELD_NOT_PRESENT, getRecord( pageCache, neoStore, LAST_TRANSACTION_ID ) );
        assertEquals( FIELD_NOT_PRESENT, getRecord( pageCache, neoStore, LAST_TRANSACTION_CHECKSUM ) );
        assertEquals( FIELD_NOT_PRESENT, getRecord( pageCache, neoStore, LAST_TRANSACTION_COMMIT_TIMESTAMP ) );
        // ... and with migrator
        StoreMigrator migrator = new StoreMigrator( fileSystemRule.get(), pageCache, config, logService );
        TransactionId actual = migrator.extractTransactionIdInformation( neoStore, storeDir, txId );

        // then
        assertEquals( txId, actual.transactionId() );
        assertEquals( TransactionIdStore.BASE_TX_CHECKSUM, actual.checksum() );
        assertEquals( TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP, actual.commitTimestamp() );
    }

    @Test
    public void writeAndReadLastTxInformation() throws IOException
    {
        StoreMigrator migrator = newStoreMigrator();
        TransactionId writtenTxId = new TransactionId( random.nextLong(), random.nextLong(), random.nextLong() );

        migrator.writeLastTxInformation( directory.graphDbDir(), writtenTxId );

        TransactionId readTxId = migrator.readLastTxInformation( directory.graphDbDir() );

        assertEquals( writtenTxId, readTxId );
    }

    @Test
    public void writeAndReadLastTxLogPosition() throws IOException
    {
        StoreMigrator migrator = newStoreMigrator();
        LogPosition writtenLogPosition = new LogPosition( random.nextLong(), random.nextLong() );

        migrator.writeLastTxLogPosition( directory.graphDbDir(), writtenLogPosition );

        LogPosition readLogPosition = migrator.readLastTxLogPosition( directory.graphDbDir() );

        assertEquals( writtenLogPosition, readLogPosition );
    }

    @Test
    public void shouldNotMigrateFilesForVersionsWithSameCapability() throws Exception
    {
        // Prepare migrator and file
        StoreMigrator migrator = newStoreMigrator();
        File graphDbDir = directory.graphDbDir();
        File neoStore = new File( graphDbDir, DEFAULT_NAME );
        neoStore.createNewFile();

        // Monitor what happens
        MySection progressMonitor = new MySection();
        // Migrate with two storeversions that have the same FORMAT capabilities
        migrator.migrate( graphDbDir, directory.directory( "migrationDir" ), progressMonitor,
                StandardV3_0.STORE_VERSION, StandardV3_2.STORE_VERSION );

        // Should not have started any migration
        assertFalse( progressMonitor.started );
    }

    private StoreMigrator newStoreMigrator()
    {
        return new StoreMigrator( fileSystemRule, pageCache,
                Config.defaults(), NullLogService.getInstance() );
    }

    private static class MySection implements MigrationProgressMonitor.Section
    {
        public boolean started;

        @Override
        public void start( long max )
        {
            started = true;
        }

        @Override
        public void progress( long add )
        {

        }

        @Override
        public void completed()
        {

        }
    }
}
