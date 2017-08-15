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

import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.kernel.configuration.Config;

public class OnlineBackupConfigurationOverride
{
    final boolean checkGraph;
    final boolean checkIndexes;
    final boolean checkLabelScanStore;
    final boolean checkPropertyOwners;

    public OnlineBackupConfigurationOverride( boolean checkGraph, boolean checkIndexes, boolean checkLabelScanStore, boolean checkPropertyOwners )
    {
        this.checkGraph = checkGraph;
        this.checkIndexes = checkIndexes;
        this.checkLabelScanStore = checkLabelScanStore;
        this.checkPropertyOwners = checkPropertyOwners;
    }

}
