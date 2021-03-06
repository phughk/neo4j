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
package org.neo4j.cypher.internal.v3_3.executionplan;

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionMode;
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.Completable;
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription;
import org.neo4j.cypher.result.QueryResult;

public interface GeneratedQueryExecution
{
    <E extends Exception> void accept( QueryResult.QueryResultVisitor<E> visitor ) throws E;

    ExecutionMode executionMode();

    InternalPlanDescription executionPlanDescription();

    String[] fieldNames();

    void setCompletable( Completable completable );
}
