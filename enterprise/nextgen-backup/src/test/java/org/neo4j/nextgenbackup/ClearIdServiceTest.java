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

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.nextgenbackup.TestHelpers.executionIsExpectedToFail;

public class ClearIdServiceTest
{
    FileSystemAbstraction fileSystemAbstraction;
    File validFile = new File( "valid.id" );
    File invalidFile = new File( "file.ab" );
    File directoryThatPassesFilter = new File( "directory.id" );
    File expectedSearchLocation = new File( "idStoreDirectory/" );

    IdGeneratorDelegator idGeneratorDelegator = mock( IdGeneratorDelegator.class );

    ClearIdService subject;

    @Before
    public void setup()
    {
        fileSystemAbstraction = mock( FileSystemAbstraction.class );
        subject = new ClearIdService( idGeneratorDelegator );
    }

    @Test
    public void filesMatchingIdFilterAreRemoved()
    {
        // given
        filesystemContainsFiles( validFile, invalidFile );

        // when
        subject.clearIdFiles( fileSystemAbstraction, expectedSearchLocation );

        // then
        verify( fileSystemAbstraction ).deleteFile( validFile );
    }

    @Test
    public void nonIdFilesAreNotCleared()
    {
        // given
        filesystemContainsFiles( invalidFile );

        // when
        subject.clearIdFiles( fileSystemAbstraction, expectedSearchLocation );

        // then
        verify( fileSystemAbstraction ).listFiles( expectedSearchLocation );
        verify( fileSystemAbstraction, times( 0 ) ).deleteFile( invalidFile );
    }

    @Test
    public void directoriesAreNotRemoved()
    {
        // given
        filesystemContainsFiles( directoryThatPassesFilter );
        makeDirectory( directoryThatPassesFilter );

        // when
        subject.clearIdFiles( fileSystemAbstraction, expectedSearchLocation );

        // then
        verify( fileSystemAbstraction ).listFiles( expectedSearchLocation );
        verify( fileSystemAbstraction, times( 0 ) ).deleteFile( directoryThatPassesFilter );
    }

    @Test
    public void indexFilesAreGeneratedWithHighestId()
    {
        // given
        Long expectedHighestId = 123L;

        // and
        filesystemContainsFiles( validFile );

        // and
        try
        {
            stub( idGeneratorDelegator.readHighId( fileSystemAbstraction, validFile ) ).toReturn( expectedHighestId );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }

        // when
        subject.clearIdFiles( fileSystemAbstraction, expectedSearchLocation );

        // then
        verify( idGeneratorDelegator ).createGenerator( fileSystemAbstraction, validFile, expectedHighestId, true );
    }

    @Test
    public void ioExceptionsAreWrappedInUncheckedException()
    {
        // given
        filesystemContainsFiles( validFile );

        // and
        try
        {
            stub( idGeneratorDelegator.readHighId( any(), any() ) ).toThrow( new IOException( "Message" ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Configuring mock shouldn't thrown exception" );
        }

        // when
        Exception exceptionThrown = executionIsExpectedToFail( () -> subject.clearIdFiles( fileSystemAbstraction, expectedSearchLocation ) );

        // then
        assertEquals( RuntimeException.class, exceptionThrown.getClass() );
        assertEquals( IOException.class, exceptionThrown.getCause().getClass() );
    }

    private void makeDirectory( File directory )
    {
        stub( fileSystemAbstraction.isDirectory( directory ) ).toReturn( true );
    }

    private void filesystemContainsFiles( File... files )
    {
        stub( fileSystemAbstraction.listFiles( any() ) ).toReturn( files );
    }
}
