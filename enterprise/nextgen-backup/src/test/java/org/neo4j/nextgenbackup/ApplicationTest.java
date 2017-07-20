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

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.File;

import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.nextgenbackup.TestHelpers.executionIsExpectedToFail;

public class ApplicationTest
{
    AdvertisedSocketAddress fromAddress = new AdvertisedSocketAddress( "hostname", 1234 );
    BackupDelegator backupDelegator;
    File storeDir;
    FileSystemAbstraction fileSystemAbstraction;

    Application subject;

    @Before
    public void setup()
    {
        storeDir = new File( "testStoreFile" );
        fileSystemAbstraction = new EphemeralFileSystemAbstraction();
        backupDelegator = mock( BackupDelegator.class );
        subject = new Application( storeDir, fileSystemAbstraction, fromAddress, backupDelegator );
    }

    @Test
    public void storeCopyLifecycleIsManaged()
    {
        // when
        subject.run();

        // then
        verify( backupDelegator ).start();
        verify( backupDelegator ).stop();
    }

    @Test
    public void backupsAreRetrievedFromRemoteServerByCorrectStoreId()
    {
        // given
        StoreId storeId = aStoreIdIsAvailableByFetch( backupDelegator, fromAddress );

        // when
        subject.run();

        // then
        verify( backupDelegator ).retrieveStore( storeId );
    }

    @Test
    public void lifecycleIsManagedEvenIfStoreIdRetrievalFails()
    {
        // given
        when( backupDelegator.fetchStoreId( any() ) ).thenThrow( RuntimeException.class );

        // when
        executionIsExpectedToFail( subject );

        // then
        verify( backupDelegator ).stop();
    }

    @Test
    public void transactionsAreRetrieved()
    {
        // given
        StoreId storeId = aStoreIdIsAvailableByFetch( backupDelegator, fromAddress );

        // when
        subject.run();

        // then
        verify( backupDelegator ).tryCatchingUp( fromAddress, storeId, storeDir );
    }

    @Test
    public void failingToRetrieveTransactionsDoesNotInterfereWithLifecycle()
    {
        // given
        StoreId storeId = aStoreIdIsAvailableByFetch( backupDelegator, fromAddress );

        // and
        when( backupDelegator.tryCatchingUp( fromAddress, storeId, storeDir ) ).thenThrow( RuntimeException.class );

        // when
        executionIsExpectedToFail( subject );

        // then
        verify( backupDelegator ).stop();
    }

    @Test
    public void idFilesCopiedFromStoresAreClearedAfterCopy()
    {
        // when
        subject.run();

        // then
        verify( backupDelegator ).clearIdFiles( fileSystemAbstraction, storeDir );
    }

    @Test
    public void failingToRemoveIdFilesDoesNotInterfereWithLifecycle()
    {
        // given
        doThrow( RuntimeException.class ).when( backupDelegator ).clearIdFiles( any(), eq( storeDir ) );

        // when
        executionIsExpectedToFail( subject );

        // then
        InOrder inOrder = inOrder( backupDelegator );
        inOrder.verify( backupDelegator ).clearIdFiles( any(), eq( storeDir ) );
        inOrder.verify( backupDelegator ).stop();
    }

    @Test
    public void afterCopyingTransactionsDatabaseIsStartedForRecovery()
    {
        // when
        subject.run();

        // then
        verify( backupDelegator ).fixLocalStorage( storeDir );
    }

    // ----------------------------------------------

    private static StoreId aStoreIdIsAvailableByFetch( BackupDelegator backupDelegator, AdvertisedSocketAddress fromAddress )
    {
        StoreId storeId = new StoreId( 1, 2, 3, 4 );
        stub( backupDelegator.fetchStoreId( fromAddress ) ).toReturn( storeId );
        return storeId;
    }
}
