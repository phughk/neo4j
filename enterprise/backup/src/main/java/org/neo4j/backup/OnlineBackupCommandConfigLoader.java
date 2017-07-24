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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.configuration.ConfigLoader;

public class OnlineBackupCommandConfigLoader
{
    private final Path homeDir;
    private final Path configDir;

    public OnlineBackupCommandConfigLoader( Path configDir, Path homeDir )
    {
        this.configDir = configDir;
        this.homeDir = homeDir;
    }

    CommandFailedSupplier<Config> loadConfig( Optional<Path> additionalConfig )
    {
        //noinspection unchecked
        return () -> withAdditionalConfig( additionalConfig,
                ConfigLoader.loadConfigWithConnectorsDisabled(
                        Optional.of( homeDir.toFile() ),
                        Optional.of( configDir.resolve( "neo4j.conf" ).toFile() ) ) );
    }

    private static Config withAdditionalConfig( Optional<Path> additionalConfig, Config config ) throws CommandFailed
    {
        if ( additionalConfig.isPresent() )
        {
            try
            {
                return config.with( MapUtil.load( additionalConfig.get().toFile() ) );
            }
            catch ( IOException e )
            {
                throw new CommandFailed( "Could not read additional config from " + additionalConfig.get(), e );
            }
        }
        return config;
    }
}
