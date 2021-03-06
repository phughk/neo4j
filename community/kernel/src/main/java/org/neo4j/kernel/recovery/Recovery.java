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
package org.neo4j.kernel.recovery;

import java.io.IOException;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static org.neo4j.storageengine.api.TransactionApplicationMode.RECOVERY;
import static org.neo4j.storageengine.api.TransactionApplicationMode.REVERSE_RECOVERY;

/**
 * This is the process of doing a recovery on the transaction log and store, and is executed
 * at startup of {@link org.neo4j.kernel.NeoStoreDataSource}.
 */
public class Recovery extends LifecycleAdapter
{
    public interface Monitor
    {
        default void recoveryRequired( LogPosition recoveryPosition )
        { // no-op by default
        }

        default void transactionRecovered( long txId )
        { // no-op by default
        }

        default void recoveryCompleted( int numberOfRecoveredTransactions )
        { // no-op by default
        }

        default void reverseStoreRecoveryCompleted( long lowestRecoveredTxId )
        { // no-op by default
        }
    }

    public interface RecoveryApplier extends Visitor<CommittedTransactionRepresentation,Exception>, AutoCloseable
    {
    }

    public interface SPI
    {
        TransactionCursor getTransactions( LogPosition recoveryFromPosition ) throws IOException;

        TransactionCursor getTransactionsInReverseOrder( LogPosition recoveryFromPosition ) throws IOException;

        LogPosition getPositionToRecoverFrom() throws IOException;

        void startRecovery();

        RecoveryApplier getRecoveryApplier( TransactionApplicationMode mode ) throws Exception;

        void allTransactionsRecovered( CommittedTransactionRepresentation lastRecoveredTransaction,
                LogPosition positionAfterLastRecoveredTransaction ) throws Exception;
    }

    private final SPI spi;
    private final Monitor monitor;
    private int numberOfRecoveredTransactions;

    private boolean recoveredLog;

    public Recovery( SPI spi, Monitor monitor )
    {
        this.spi = spi;
        this.monitor = monitor;
    }

    @Override
    public void init() throws Throwable
    {
        LogPosition recoveryFromPosition = spi.getPositionToRecoverFrom();
        if ( LogPosition.UNSPECIFIED.equals( recoveryFromPosition ) )
        {
            return;
        }

        monitor.recoveryRequired( recoveryFromPosition );
        spi.startRecovery();

        // Backwards for neo store only
        long lowestRecoveredTxId = TransactionIdStore.BASE_TX_ID;
        try ( TransactionCursor transactionsToRecover = spi.getTransactionsInReverseOrder( recoveryFromPosition );
              RecoveryApplier recoveryVisitor = spi.getRecoveryApplier( REVERSE_RECOVERY ); )
        {
            while ( transactionsToRecover.next() )
            {
                recoveryVisitor.visit( transactionsToRecover.get() );
                lowestRecoveredTxId = transactionsToRecover.get().getCommitEntry().getTxId();
            }
        }

        monitor.reverseStoreRecoveryCompleted( lowestRecoveredTxId );

        // Forward with all appliers
        LogPosition recoveryToPosition;
        CommittedTransactionRepresentation lastTransaction = null;
        try ( TransactionCursor transactionsToRecover = spi.getTransactions( recoveryFromPosition );
              RecoveryApplier recoveryVisitor = spi.getRecoveryApplier( RECOVERY ); )
        {
            while ( transactionsToRecover.next() )
            {
                lastTransaction = transactionsToRecover.get();
                long txId = lastTransaction.getCommitEntry().getTxId();
                recoveryVisitor.visit( lastTransaction );
                monitor.transactionRecovered( txId );
                numberOfRecoveredTransactions++;
            }
            recoveryToPosition = transactionsToRecover.position();
        }

        if ( recoveryToPosition.equals( LogPosition.UNSPECIFIED ) )
        {
            recoveryToPosition = recoveryFromPosition;
        }

        spi.allTransactionsRecovered( lastTransaction, recoveryToPosition );
        recoveredLog = true;
    }

    @Override
    public void start() throws Throwable
    {
        // This is here as by now all other services have reacted to the recovery process
        if ( recoveredLog )
        {
            monitor.recoveryCompleted( numberOfRecoveredTransactions );
        }
    }
}
