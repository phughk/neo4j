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

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;

public class ClearIdService
{
    IdGeneratorDelegator idGeneratorDelegator;

    public ClearIdService( IdGeneratorDelegator idGeneratorDelegator )
    {
        this.idGeneratorDelegator = idGeneratorDelegator;
    }

    public void clearIdFiles( FileSystemAbstraction fileSystem, File targetDirectory )
    {
        try
        {
            for ( File file : fileSystem.listFiles( targetDirectory ) )
            {
                if ( !fileSystem.isDirectory( file ) && file.getName().endsWith( ".id" ) )
                {
                    long highId = idGeneratorDelegator.readHighId( fileSystem, file );
                    fileSystem.deleteFile( file );
                    idGeneratorDelegator.createGenerator( fileSystem, file, highId, true );
                }
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
