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

import java.nio.file.Path;
import java.util.Optional;

import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.TimeUtil;


public class OnlineBackupRequiredArguments
{
    final HostnamePort address;
    final Path folder;
    final String name;
    final boolean fallbackToFull;
    final boolean doConsistencyCheck;
    final long timeout;
    final Optional<Path> additionalConfig;
    final Path reportDir;

    public OnlineBackupRequiredArguments( HostnamePort address, Path folder, String name, boolean fallbackToFull, boolean doConsistencyCheck, long timeout,
            Optional<Path> additionalConfig, Path reportDir )
    {
        this.address = address;
        this.folder = folder;
        this.name = name;
        this.fallbackToFull = fallbackToFull;
        this.doConsistencyCheck = doConsistencyCheck;
        this.timeout = timeout;
        this.additionalConfig = additionalConfig;
        this.reportDir = reportDir;
    }

    public AdvertisedSocketAddress addressAsAdvertised()
    {
        return new AdvertisedSocketAddress( address.getHost(), address.getPort() );
    }
}
