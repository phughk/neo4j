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
package org.neo4j.cypher.internal.ir.v3_3

import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.ir.v3_3.helpers.ExpressionConverters._

import scala.collection.mutable.ArrayBuffer
import scala.collection.{GenTraversableOnce, mutable}
import scala.runtime.ScalaRunTime

case class QueryGraph(// !!! If you change anything here, make sure to update the equals method at the bottom of this class !!!
                      patternRelationships: Set[PatternRelationship] = Set.empty,
                      patternNodes: Set[String] = Set.empty,
                      argumentIds: Set[String] = Set.empty,
                      selections: Selections = Selections(),
                      optionalMatches: IndexedSeq[QueryGraph] = Vector.empty,
                      hints: Set[Hint] = Set.empty,
                      shortestPathPatterns: Set[ShortestPathPattern] = Set.empty,
                      mutatingPatterns: IndexedSeq[MutatingPattern] = IndexedSeq.empty)
  extends UpdateGraph {

  def smallestGraphIncluding(mustInclude: Set[String]): Set[String] = {
    if (mustInclude.size < 2)
      mustInclude intersect allCoveredIds
    else {
      var accumulatedElements = mustInclude
      for {
        lhs <- mustInclude
        rhs <- mustInclude
        if lhs < rhs
      } {
        accumulatedElements ++= findPathBetween(lhs, rhs)
      }
      accumulatedElements
    }
  }

  def dependencies: Set[String] =
    optionalMatches.flatMap(_.dependencies).toSet ++
      selections.predicates.flatMap(_.dependencies) ++
      mutatingPatterns.flatMap(_.dependencies) ++
      argumentIds

  private def findPathBetween(startFromL: String, startFromR: String): Set[String] = {
    var l = Seq(PathSoFar(startFromL, Set.empty))
    var r = Seq(PathSoFar(startFromR, Set.empty))
    (0 to patternRelationships.size) foreach { i =>
      if (i % 2 == 0) {
        l = expand(l)
        val matches = hasExpandedInto(l, r)
        if (matches.nonEmpty)
          return matches.head
      }
      else {
        r = expand(r)
        val matches = hasExpandedInto(r, l)
        if (matches.nonEmpty)
          return matches.head
      }
    }

    // Did not find any path. Let's do the safe thing and return everything
    patternRelationships.flatMap(_.coveredIds)
  }

  private def hasExpandedInto(from: Seq[PathSoFar], into: Seq[PathSoFar]): Seq[Set[String]] =
    for {lhs <- from
         rhs <- into
         if rhs.alreadyVisited.exists(p => p.coveredIds.contains(lhs.end))}
      yield {
        (lhs.alreadyVisited ++ rhs.alreadyVisited).flatMap(_.coveredIds)
      }

  private case class PathSoFar(end: String, alreadyVisited: Set[PatternRelationship]) {
    def coveredIds: Set[String] = alreadyVisited.flatMap(_.coveredIds) + end
  }

  private def expand(from: Seq[PathSoFar]): Seq[PathSoFar] = {
    from.flatMap {
      case PathSoFar(end, alreadyVisited) =>
        patternRelationships.collect {
          case pr if !alreadyVisited(pr) && pr.coveredIds(end) =>
            PathSoFar(pr.otherSide(end), alreadyVisited + pr)
        }
    }
  }

  def size: Int = patternRelationships.size

  def isEmpty: Boolean = this == QueryGraph.empty

  def nonEmpty: Boolean = !isEmpty

  def mapSelections(f: Selections => Selections): QueryGraph =
    copy(selections = f(selections), optionalMatches = optionalMatches.map(_.mapSelections(f)))

  def addPatternNodes(nodes: String*): QueryGraph = copy(patternNodes = patternNodes ++ nodes)

  def addPatternRelationship(rel: PatternRelationship): QueryGraph =
    copy(
      patternNodes = patternNodes + rel.nodes._1 + rel.nodes._2,
      patternRelationships = patternRelationships + rel
    )

  def addPatternRelationships(rels: Seq[PatternRelationship]): QueryGraph =
    rels.foldLeft[QueryGraph](this)((qg, rel) => qg.addPatternRelationship(rel))

  def addShortestPath(shortestPath: ShortestPathPattern): QueryGraph = {
    val rel = shortestPath.rel
    copy(
      patternNodes = patternNodes + rel.nodes._1 + rel.nodes._2,
      shortestPathPatterns = shortestPathPatterns + shortestPath
    )
  }

  /*
  Includes not only pattern nodes in the read part of the query graph, but also pattern nodes from CREATE and MERGE
   */
  def allPatternNodes: collection.Set[String] = {
    val nodes = mutable.Set[String]()
    collectAllPatternNodes(nodes.add)
    nodes
  }

  def collectAllPatternNodes(f: (String) => Unit): Unit = {
    patternNodes.foreach(f)
    optionalMatches.foreach(m => m.allPatternNodes.foreach(f))
    createNodePatterns.foreach(p => f(p.nodeName))
    mergeNodePatterns.foreach(p => f(p.createNodePattern.nodeName))
    mergeRelationshipPatterns.foreach(p => p.createNodePatterns.foreach(pp => f(pp.nodeName)))
  }

  def allPatternRelationshipsRead: Set[PatternRelationship] =
    patternRelationships ++ optionalMatches.flatMap(_.allPatternRelationshipsRead)

  def allPatternNodesRead: Set[String] =
    patternNodes ++ optionalMatches.flatMap(_.allPatternNodesRead)

  def addShortestPaths(shortestPaths: ShortestPathPattern*): QueryGraph = shortestPaths.foldLeft(this)((qg, p) => qg.addShortestPath(p))

  def addArgumentId(newId: String): QueryGraph = copy(argumentIds = argumentIds + newId)

  def addArgumentIds(newIds: Seq[String]): QueryGraph = copy(argumentIds = argumentIds ++ newIds)

  def addSelections(selections: Selections): QueryGraph =
    copy(selections = Selections(selections.predicates ++ this.selections.predicates))

  def addPredicates(predicates: Expression*): QueryGraph = {
    val newSelections = Selections(predicates.flatMap(_.asPredicates).toSet)
    copy(selections = selections ++ newSelections)
  }

  def addHints(addedHints: GenTraversableOnce[Hint]): QueryGraph = {
    copy(hints = hints ++ addedHints)
  }

  def withoutHints(hintsToIgnore: GenTraversableOnce[Hint]): QueryGraph = copy(hints = hints -- hintsToIgnore)

  def withoutArguments(): QueryGraph = withArgumentIds(Set.empty)

  def withArgumentIds(newArgumentIds: Set[String]): QueryGraph =
    copy(argumentIds = newArgumentIds)

  def withAddedOptionalMatch(optionalMatch: QueryGraph): QueryGraph = {
    val argumentIds = allCoveredIds intersect optionalMatch.allCoveredIds
    copy(optionalMatches = optionalMatches :+ optionalMatch.addArgumentIds(argumentIds.toIndexedSeq))
  }

  def withOptionalMatches(optionalMatches: IndexedSeq[QueryGraph]): QueryGraph = {
    copy(optionalMatches = optionalMatches)
  }

  def withMergeMatch(matchGraph: QueryGraph): QueryGraph = {
    if (mergeQueryGraph.isEmpty) throw new IllegalArgumentException("Don't add a merge to this non-merge QG")

    // NOTE: Merge can only contain one mutating pattern
    assert(mutatingPatterns.length == 1)
    val newMutatingPattern = mutatingPatterns.collectFirst {
      case p: MergeNodePattern => p.copy(matchGraph = matchGraph)
      case p: MergeRelationshipPattern => p.copy(matchGraph = matchGraph)
    }.get

    copy(argumentIds = matchGraph.argumentIds, mutatingPatterns = IndexedSeq(newMutatingPattern))
  }

  def withSelections(selections: Selections): QueryGraph = copy(selections = selections)

  def withPatternRelationships(patterns: Set[PatternRelationship]): QueryGraph =
    copy(patternRelationships = patterns)

  def withPatternNodes(nodes: Set[String]): QueryGraph =
    copy(patternNodes = nodes)

  def knownProperties(idName: String): Set[Property] =
    selections.propertyPredicatesForSet.getOrElse(idName, Set.empty)

  private def knownLabelsOnNode(node: String): Set[LabelName] =
    selections
      .labelPredicates.getOrElse(node, Set.empty)
      .flatMap(_.labels)

  def allKnownLabelsOnNode(node: String): Set[LabelName] =
    knownLabelsOnNode(node) ++ optionalMatches.flatMap(_.allKnownLabelsOnNode(node))

  def allKnownPropertiesOnIdentifier(idName: String): Set[Property] =
    knownProperties(idName) ++ optionalMatches.flatMap(_.allKnownPropertiesOnIdentifier(idName))

  def allKnownNodeProperties: Set[Property] = {
    val matchedNodes = patternNodes ++ patternRelationships.flatMap(r => Set(r.nodes._1, r.nodes._2))
    matchedNodes.flatMap(knownProperties) ++ optionalMatches.flatMap(_.allKnownNodeProperties)
  }

  def allKnownRelProperties: Set[Property] =
    patternRelationships.map(_.name).flatMap(knownProperties) ++ optionalMatches.flatMap(_.allKnownRelProperties)


  def findRelationshipsEndingOn(id: String): Set[PatternRelationship] =
    patternRelationships.filter { r => r.left == id || r.right == id }

  def allPatternRelationships: Set[PatternRelationship] =
    patternRelationships ++ optionalMatches.flatMap(_.allPatternRelationships) ++
      // Recursively add relationships from the merge-read-part
      mergeNodePatterns.flatMap(_.matchGraph.allPatternRelationships) ++
      mergeRelationshipPatterns.flatMap(_.matchGraph.allPatternRelationships)

  def coveredIdsExceptArguments: Set[String] = {
    val patternIds = QueryGraph.coveredIdsForPatterns(patternNodes, patternRelationships)
    patternIds ++ selections.predicates.flatMap(_.dependencies)
  }

  def coveredIds: Set[String] = {
    val patternIds = QueryGraph.coveredIdsForPatterns(patternNodes, patternRelationships)
    patternIds ++ argumentIds ++ selections.predicates.flatMap(_.dependencies)
  }

  def allCoveredIds: Set[String] = {
    val otherSymbols = optionalMatches.flatMap(_.allCoveredIds) ++ mutatingPatterns.flatMap(_.coveredIds)
    coveredIds ++ otherSymbols
  }

  val allHints: Set[Hint] =
    if (optionalMatches.isEmpty) hints else hints ++ optionalMatches.flatMap(_.allHints)

  def ++(other: QueryGraph): QueryGraph =
    QueryGraph(
      selections = selections ++ other.selections,
      patternNodes = patternNodes ++ other.patternNodes,
      patternRelationships = patternRelationships ++ other.patternRelationships,
      optionalMatches = optionalMatches ++ other.optionalMatches,
      argumentIds = argumentIds ++ other.argumentIds,
      hints = hints ++ other.hints,
      shortestPathPatterns = shortestPathPatterns ++ other.shortestPathPatterns,
      mutatingPatterns = mutatingPatterns ++ other.mutatingPatterns
    )

  def isCoveredBy(other: QueryGraph): Boolean = {
    patternNodes.subsetOf(other.patternNodes) &&
      patternRelationships.subsetOf(other.patternRelationships) &&
      argumentIds.subsetOf(other.argumentIds) &&
      optionalMatches.toSet.subsetOf(other.optionalMatches.toSet) &&
      selections.predicates.subsetOf(other.selections.predicates) &&
      shortestPathPatterns.subsetOf(other.shortestPathPatterns)
  }

  def covers(other: QueryGraph): Boolean = other.isCoveredBy(this)

  def hasOptionalPatterns: Boolean = optionalMatches.nonEmpty

  def patternNodeLabels: Map[String, Set[LabelName]] =
    patternNodes.collect { case node: String => node -> selections.labelsOnNode(node) }.toMap

  /**
    * Returns the connected patterns of this query graph where each connected pattern is represented by a QG.
    * Does not include optional matches, shortest paths or predicates that have dependencies across multiple of the
    * connected query graphs.
    */
  def connectedComponents: Seq[QueryGraph] = {
    val visited = mutable.Set.empty[String]

    def createComponentQueryGraphStartingFrom(patternNode: String) = {
      val qg = connectedComponentFor(patternNode, visited)
      val coveredIds = qg.coveredIds
      val shortestPaths = shortestPathPatterns.filter {
        p => coveredIds.contains(p.rel.nodes._1) && coveredIds.contains(p.rel.nodes._2)
      }
      val shortestPathIds = shortestPaths.flatMap(p => Set(p.rel.name) ++ p.name)
      val allIds = coveredIds ++ argumentIds ++ shortestPathIds
      val predicates = selections.predicates.filter(_.dependencies.subsetOf(allIds))
      val filteredHints = hints.filter(h => h.variables.forall(variable => coveredIds.contains(variable.name)))
      qg.
        withSelections(Selections(predicates)).
        withArgumentIds(argumentIds).
        addHints(filteredHints).
        addShortestPaths(shortestPaths.toIndexedSeq: _*)
    }

    /*
    We want the components that have patterns connected to arguments to be planned first, so we do not pull in arguments
    to other components by mistake
     */
    val argumentComponents = (patternNodes intersect argumentIds).toIndexedSeq.collect {
      case patternNode if !visited(patternNode) =>
        createComponentQueryGraphStartingFrom(patternNode)
    }

    val rest = patternNodes.toIndexedSeq.collect {
      case patternNode if !visited(patternNode) =>
        createComponentQueryGraphStartingFrom(patternNode)
    }

    argumentComponents ++ rest
  }

  def withoutPatternRelationships(patterns: Set[PatternRelationship]): QueryGraph =
    copy(patternRelationships = patternRelationships -- patterns)

  def joinHints: Set[UsingJoinHint] =
    hints.collect { case hint: UsingJoinHint => hint }

  private def connectedComponentFor(startNode: String, visited: mutable.Set[String]): QueryGraph = {
    val queue = mutable.Queue(startNode)
    var qg = QueryGraph.empty
    while (queue.nonEmpty) {
      val node = queue.dequeue()
      if (!visited(node)) {
        visited += node

        val filteredPatterns = patternRelationships.filter { rel =>
          rel.coveredIds.contains(node) && !qg.patternRelationships.contains(rel)
        }

        val patternsWithSameName =
          patternRelationships.filterNot(filteredPatterns).filter { r => filteredPatterns.exists(_.name == r.name) }

        queue.enqueue(filteredPatterns.toIndexedSeq.map(_.otherSide(node)): _*)
        queue.enqueue(patternsWithSameName.toIndexedSeq.flatMap(r => Seq(r.left, r.right)): _*)

        val patternsInConnectedComponent = filteredPatterns ++ patternsWithSameName
        qg = qg
          .addPatternNodes(node)
          .addPatternRelationships(patternsInConnectedComponent.toIndexedSeq)

        val alreadyHaveArguments = qg.argumentIds.nonEmpty

        if (!alreadyHaveArguments && (argumentsOverLapsWith(qg.coveredIds) || predicatePullsInArguments(node))) {
          qg = qg.withArgumentIds(argumentIds)
          val nodesSolvedByArguments = patternNodes intersect qg.argumentIds
          queue.enqueue(nodesSolvedByArguments.toIndexedSeq: _*)
        }
      }
    }
    qg
  }

  private def argumentsOverLapsWith(coveredIds: Set[String]) = (argumentIds intersect coveredIds).nonEmpty

  private def predicatePullsInArguments(node: String) = selections.flatPredicates.exists { p =>
    val deps = p.dependencies.map(_.name)
    deps(node) && (deps intersect argumentIds).nonEmpty
  }

  def containsReads: Boolean = {
    (patternNodes -- argumentIds).nonEmpty ||
      patternRelationships.nonEmpty ||
      selections.nonEmpty ||
      shortestPathPatterns.nonEmpty ||
      optionalMatches.nonEmpty ||
      containsMergeRecursive
  }

  def writeOnly: Boolean = !containsReads && containsUpdates

  def addMutatingPatterns(pattern: MutatingPattern): QueryGraph = {
    val copyPatterns = new mutable.ArrayBuffer[MutatingPattern](mutatingPatterns.size + 1)
    copyPatterns.appendAll(mutatingPatterns)
    copyPatterns.append(pattern)

    copy(mutatingPatterns = copyPatterns)
  }

  def addMutatingPatterns(patterns: Seq[MutatingPattern]): QueryGraph = {
    val copyPatterns = new ArrayBuffer[MutatingPattern](patterns.size)
    copyPatterns.appendAll(mutatingPatterns)
    copyPatterns.appendAll(patterns)
    copy(mutatingPatterns = copyPatterns)
  }

  /**
    * We have to do this special treatment of QG to avoid problems when checking that the produced plan actually
    * solves what we set out to solve. In some rare circumstances, we'll get a few optional matches that are independent of each other.
    *
    * Given the way our planner works, it can unpredictably plan these optional matches in different orders, which leads to an exception being thrown when
    * checking that the correct query has been solved.
    */
  override def equals(in: scala.Any): Boolean = in match {
    case other: QueryGraph if other canEqual this =>

      val optionals = if (optionalMatches.isEmpty) {
        true
      } else {
        compareOptionalMatches(other)
      }

      patternRelationships == other.patternRelationships &&
        patternNodes == other.patternNodes &&
        argumentIds == other.argumentIds &&
        selections == other.selections &&
        optionals &&
        hints == other.hints &&
        shortestPathPatterns == other.shortestPathPatterns &&
        mutatingPatterns == other.mutatingPatterns

    case _ =>
      false
  }

  override def canEqual(that: Any): Boolean = that.isInstanceOf[QueryGraph]

  override def hashCode(): Int = {
    val optionals = if(optionalMatches.nonEmpty && containsIndependentOptionalMatches)
      optionalMatches.toSet
    else
      optionalMatches

    ScalaRunTime._hashCode((patternRelationships, patternNodes, argumentIds, selections, optionals, hints, shortestPathPatterns, mutatingPatterns))
  }

  private lazy val containsIndependentOptionalMatches = {
    val nonOptional = coveredIdsExceptArguments

    val result = this.optionalMatches.foldLeft(false) {
      case (acc, oqg) =>
        acc || (oqg.dependencies -- nonOptional).nonEmpty
    }

    result
  }

  private def compareOptionalMatches(other: QueryGraph) = {
    if (containsIndependentOptionalMatches) {
      optionalMatches.toSet == other.optionalMatches.toSet
    } else
      optionalMatches == other.optionalMatches
  }
}

object QueryGraph {
  val empty = QueryGraph()

  def coveredIdsForPatterns(patternNodeIds: Set[String], patternRels: Set[PatternRelationship]): Set[String] = {
    val patternRelIds = patternRels.flatMap(_.coveredIds)
    patternNodeIds ++ patternRelIds
  }

  implicit object byCoveredIds extends Ordering[QueryGraph] {

    import scala.math.Ordering.Implicits

    def compare(x: QueryGraph, y: QueryGraph): Int = {
      val xs = x.coveredIds.toIndexedSeq.sorted
      val ys = y.coveredIds.toIndexedSeq.sorted
      Implicits.seqDerivedOrdering[Seq, String].compare(xs, ys)
    }
  }
}
