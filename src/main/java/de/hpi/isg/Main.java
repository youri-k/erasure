package de.hpi.isg;

import de.hpi.isg.RelationalDependencyRules.Rule;
import de.hpi.isg.RelationalDependencyRules.Attribute;
import de.hpi.isg.RelationalDependencyRules.Cell;
import de.hpi.isg.RelationalDependencyRules.Cell.HyperEdge;
import com.gurobi.gurobi.*;
import org.apache.commons.csv.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

public class Main {
    final static ArrayList<Rule> rules = new ArrayList<>();
    final static HashMap<Attribute, ArrayList<Rule>> attributeInHead = new HashMap<>();
    final static HashMap<Attribute, ArrayList<Rule>> attributeInTail = new HashMap<>();
    final static HashMap<String, String> tableName2keyCol = new HashMap<>();
    static GRBEnv env;

    final static String dataset = "ex";
    final static int NUM_KEYS = 10;

    public static void main(String[] args) throws Exception {
        String basePath = args.length > 0 ? args[0] : "/home/y/neu/erasure/";

         env = new GRBEnv();
         env.set(GRB.IntParam.OutputFlag, 0);
         env.set(GRB.IntParam.LogToConsole, 0);

        parseRules(basePath);
        parseSchema(basePath);

        var allAttributes = new HashSet<Attribute>();
        allAttributes.addAll(attributeInTail.keySet());
        allAttributes.addAll(attributeInHead.keySet());

//        checkNonCyclicRules(allAttributes);

        var instatiator = new Instatiator(attributeInHead, attributeInTail, tableName2keyCol);

        HashSet<Attribute> testSet = new HashSet<>(List.of(new Attribute("socialgram.profile", "totalLikes"), new Attribute("socialgram.profile", "pscore")));

//        iterateAttributes(instatiator, allAttributes);
         compareBatch(instatiator, allAttributes);

//        var exampleDelete = new Cell(new Attribute("socialgram.profile", "totalLikes"), "721");
//        socialgram.profile totalLikes[1108] => 0
//        var exampleDelete = new Cell(new Attribute("socialgram.profile", "avgQuotes"), "582");
//        var exampleDelete = new Cell(new Attribute("socialgram.posts", "tweetid"), "951680840706048000");
//        var exampleDelete = new Cell(new Attribute("tax.tax", "ChildExemp"), "415701");
//        instatiator.completeCell(exampleDelete);

//        optimalDelete(exampleDelete, instatiator);
//        ilpApproach(exampleDelete, instatiator);
//        approximateDelete(exampleDelete, instatiator);
    }

    private static void checkNonCyclicRules(HashSet<Attribute> allAttributes) {
        HashSet<Attribute> checkedAttributes = new HashSet<>();
        for (var startAttribute : allAttributes) {
            if (checkedAttributes.contains(startAttribute)) {
                continue;
            }
            Queue<Attribute> attributesToVisit = new LinkedList<>();
            HashMap<Attribute, Attribute> attr2Parent = new HashMap<>();
            attributesToVisit.add(startAttribute);
            attr2Parent.put(startAttribute, new Attribute("", ""));
            checkedAttributes.add(startAttribute);

            while (!attributesToVisit.isEmpty()) {
                var curr = attributesToVisit.poll();
                for (var rule : attributeInHead.getOrDefault(curr, new ArrayList<>(0))) {
                    for (var attr : rule.tail) {
                        assert (attr2Parent.get(attr) == null || attr2Parent.get(curr).equals(attr)) || attr.equals(curr);
                        if (!attr.equals(curr)) {
                            attr2Parent.put(attr, curr);
                        }
                        if (checkedAttributes.add(attr)) {
                            attributesToVisit.add(attr);
                        }
                    }
                }
                for (var rule : attributeInTail.getOrDefault(curr, new ArrayList<>(0))) {
                    var attr = rule.head;
                    assert (attr2Parent.get(attr) == null || attr2Parent.get(curr).equals(attr)) || attr.equals(curr);
                    if (!attr.equals(curr)) {
                        attr2Parent.put(attr, curr);
                    }
                    if (checkedAttributes.add(attr)) {
                        attributesToVisit.add(attr);
                    }
                }
            }
        }
    }

    private static void iterateAttributes(Instatiator instatiator, Set<Attribute> attributes) throws Exception {
        writeHeader();
//        attributes = new HashSet<>();
//        attributes.add(new Attribute("socialgram.profile", "totalQuotes"));
        for (var attr : attributes) {
            System.out.print(attr.toString() + ",");
            var keys = getKeys(instatiator, attr);
            for (var key : keys) {
                var deletionCell = new Cell(attr, key);
                instatiator.completeCell(deletionCell);
//                optimalDelete(deletionCell, instatiator);
                var start = System.nanoTime();
                var result = approximateDelete(deletionCell, instatiator);
//                var result = optimalDelete(deletionCell, instatiator);
//                var result = ilpApproach(deletionCell, instatiator);
                var stop = System.nanoTime();
                Utils.approximateTime += stop - start;
                Utils.approximateDeletes += result.size();

                start = System.nanoTime();
                var optResult = optimalDelete(deletionCell, instatiator);
//                result = approximateDelete(deletionCell, instatiator);
                stop = System.nanoTime();
                Utils.optimalTime += stop - start;
                Utils.optimalDeletes += optResult.size();

                start = System.nanoTime();
                var ilpResult = ilpApproach(deletionCell, instatiator);
//                result = optimalDelete(deletionCell, instatiator);
//                result = approximateDelete(deletionCell, instatiator);
                stop = System.nanoTime();
                Utils.ilpTime += stop - start;
                Utils.ilpDeletes += ilpResult.size();
            }


            writeOutput();
        }

    }

    private static void compareBatch(Instatiator instatiator, Set<Attribute> attributes) throws Exception {
//        writeHeader();
        var batch = new ArrayList<Cell>(NUM_KEYS * attributes.size());
        var naiveResult = new HashSet<Cell>();
        var naiveILPResult = new HashSet<Cell>();
        long naiveTime = 0L, batchTime = 0L, naiveILP = 0L, batchILP = 0L;
        for (var attr : attributes) {
            System.out.print(attr.toString() + ",");
            var keys = getKeys(instatiator, attr);
            for (var key : keys) {
                var deletionCell = new Cell(attr, key);
                instatiator.completeCell(deletionCell);
                batch.add(deletionCell);
                var start = System.nanoTime();
                // naiveResult.addAll(optimalDelete(deletionCell, instatiator));
                var stop = System.nanoTime();
                naiveTime += stop - start;
                // naiveILPResult.addAll(ilpApproach(deletionCell, instatiator));
                naiveILP += System.nanoTime() - stop;
            }
        }
        System.out.println();
        var start = System.nanoTime();
        naiveILPResult = batchedIlpApproach(batch, instatiator, true);
        // var batchedResult = batchedOptimalDelete(batch, instatiator);
        var stop = System.nanoTime();
        naiveILP = stop - start;
        var batchedILP = batchedIlpApproach(batch, instatiator, false);
        batchILP += System.nanoTime() - stop;
//        var resultDiff = (HashSet<Cell>) naiveResult.clone();
//        resultDiff.removeAll(batchedResult);
        // System.out.println(naiveResult.size() + "," + batchedResult.size() + "," + (long) (naiveTime / 1e6) + "," + (long) (batchTime / 1e6));
        System.out.println(naiveILPResult.size() + "," + batchedILP.size() + "," + (long) (naiveILP / 1e6) + "," + (long) (batchILP / 1e6));
    }

    private static void writeHeader() {
//        System.out.println("Attributes,relativeApproximateTime,relativeOptimalTime,relativeApproximateDeletes,relativeOptimalDeletes,relativeApproximateTraversionDepth,relativeOptimalTraversionDepth,relativeApproximateNumEdges,relativeOptimalNumEdges,treeTimeShare,approximateTime,optimalTime,approximateDeletes,optimalDeletes,approximateTraversionDepth,optimalTraversionDepth,approximateNumEdges,optimalNumEdges,optimalTreeTime");
        System.out.println("Attributes,approximateTime,optimalTime,ilpTime,approximateDeletes,optimalDeletes,ilpDeletes,approximateInstantiatedCells,optimalInstantiatedCells,ilpInstantiatedCells,approximateTraversionDepth,optimalTraversionDepth,approximateNumEdges,optimalNumEdges,optimalTreeTime");
    }

    private static void writeOutput() {
        ArrayList<String> output = new ArrayList<>();
//        long maxTime = Math.max(de.hpi.isg.Utils.approximateTime, de.hpi.isg.Utils.optimalTime);
//        output.add(String.valueOf(((double) de.hpi.isg.Utils.approximateTime / maxTime * 100)));
//        output.add(String.valueOf(((double) de.hpi.isg.Utils.optimalTime / maxTime * 100)));
//        long maxDeletes = Math.max(de.hpi.isg.Utils.approximateDeletes, de.hpi.isg.Utils.optimalDeletes);
//        output.add(String.valueOf(((double) de.hpi.isg.Utils.approximateDeletes / maxDeletes * 100)));
//        output.add(String.valueOf(((double) de.hpi.isg.Utils.optimalDeletes / maxDeletes * 100)));
//        long maxTraversionDepth = Math.max(Math.max(de.hpi.isg.Utils.approximateTraversionDepth, de.hpi.isg.Utils.optimalTraversionDepth), 1);
//        output.add(String.valueOf((double) de.hpi.isg.Utils.approximateTraversionDepth / maxTraversionDepth * 100));
//        output.add(String.valueOf((double) de.hpi.isg.Utils.optimalTraversionDepth / maxTraversionDepth * 100));
//        long maxNumEdges = Math.max(Math.max(de.hpi.isg.Utils.approximateNumEdges, de.hpi.isg.Utils.optimalNumEdges), 1);
//        output.add(String.valueOf((double) de.hpi.isg.Utils.approximateNumEdges / maxNumEdges * 100));
//        output.add(String.valueOf((double) de.hpi.isg.Utils.optimalNumEdges / maxNumEdges * 100));
//        output.add(String.valueOf((double) de.hpi.isg.Utils.optimalTreeTime / de.hpi.isg.Utils.optimalTime * 100));
//        output.add(String.valueOf((long) (de.hpi.isg.Utils.approximateTime / 1e6)));
//        output.add(String.valueOf((long) (de.hpi.isg.Utils.optimalTime / 1e6)));
//        output.add(String.valueOf((long) (de.hpi.isg.Utils.ilpTime / 1e6)));
        output.add(String.valueOf(Utils.approximateDeletes));
        output.add(String.valueOf(Utils.optimalDeletes));
        output.add(String.valueOf(Utils.ilpDeletes));
//        output.add(String.valueOf(de.hpi.isg.Utils.approximateInstantiatedCells));
//        output.add(String.valueOf(de.hpi.isg.Utils.optimalInstantiatedCells));
//        output.add(String.valueOf(de.hpi.isg.Utils.ilpInstantiatedCells));
//        output.add(String.valueOf(de.hpi.isg.Utils.approximateTraversionDepth));
//        output.add(String.valueOf(de.hpi.isg.Utils.optimalTraversionDepth));
//        output.add(String.valueOf(de.hpi.isg.Utils.approximateNumEdges));
//        output.add(String.valueOf(de.hpi.isg.Utils.optimalNumEdges));
//        output.add(String.valueOf((long) (de.hpi.isg.Utils.optimalTreeTime / 1e6)));
        System.out.println(String.join(",", output));
        Utils.approximateTime = 0;
        Utils.optimalTime = 0;
        Utils.ilpTime = 0;
        Utils.approximateDeletes = 0;
        Utils.optimalDeletes = 0;
        Utils.ilpDeletes = 0;
        Utils.approximateTraversionDepth = 0;
        Utils.optimalTraversionDepth = 0;
        Utils.approximateNumEdges = 0;
        Utils.optimalNumEdges = 0;
        Utils.approximateInstantiatedCells = 0;
        Utils.optimalInstantiatedCells = 0;
        Utils.ilpInstantiatedCells = 0;
        Utils.optimalTreeTime = 0;
    }

    private static ArrayList<String> getKeys(Instatiator instatiator, Attribute attr) throws SQLException {
        ArrayList<String> keys = new ArrayList<>(NUM_KEYS);
        var resultSet = instatiator.statement.executeQuery("SELECT " + tableName2keyCol.get(attr.table) + " FROM " + attr.table + " ORDER BY RANDOM() LIMIT " + NUM_KEYS);
        while (resultSet.next()) {
            keys.add(resultSet.getString(1));
        }
        return keys;
    }

    private static boolean containsParent(HyperEdge edge, HashSet<Cell> parents) {
        for (var cell : edge) {
            if (parents.contains(cell)) {
                return true;
            }
        }
        return false;
    }

    private static HashSet<Cell> optimalDelete(Cell exampleDelete, Instatiator instatiator) throws Exception {
        var start = System.nanoTime();
        var instantiatedCells = new HashSet<Cell>();
        LinkedList<HashSet<Cell>> treeLevels = new LinkedList<>();
        HashSet<Cell> nextLevel = new HashSet<>();
        HashSet<Cell> currLevel = new HashSet<>();

        HashMap<Cell, ArrayList<HyperEdge>> cell2Edge = new HashMap<>();
        HashMap<Cell, HashSet<Cell>> cell2Parents = new HashMap<>();
        cell2Parents.put(exampleDelete, new HashSet<>(0));
        currLevel.add(exampleDelete);

        HashMap<Cell, Cell> cell2Identity = new HashMap<>();

        while (!currLevel.isEmpty()) {
            for (var curr : currLevel) {
                var result = instatiator.instantiateAttachedCells(curr, exampleDelete.insertionTime);
                instantiatedCells.add(curr);
                Utils.optimalNumEdges += result.size();
                for (var edge : result) {
                    if (!containsParent(edge, cell2Parents.get(curr))) {
                        var cellIter = edge.iterator();
                        var newCells = new ArrayList<Cell>(edge.size());
                        while (cellIter.hasNext()) {
                            var cell = cellIter.next();
                            var unifiedCell = cell2Identity.get(cell);
                            if (unifiedCell == null) {
                                cell2Identity.put(cell, cell);
                                unifiedCell = cell;
                            } else {
                                cellIter.remove();
                                newCells.add(unifiedCell);
                            }
                            cell2Parents.computeIfAbsent(unifiedCell, a -> new HashSet<>()).add(curr);
                            if (!instantiatedCells.contains(unifiedCell)) {
                                nextLevel.add(unifiedCell);
                            }
                        }
                        edge.addAll(newCells);
                        cell2Edge.computeIfAbsent(curr, a -> new ArrayList<>()).add(edge);
                    }
                }
            }
            treeLevels.addFirst(currLevel);
            currLevel = nextLevel;
            nextLevel = new HashSet<>();
        }

        var stop = System.nanoTime();
        Utils.optimalTreeTime += stop - start;
        Utils.optimalTraversionDepth += treeLevels.size();


        for (var currLevels : treeLevels) {
            for (var currCell : currLevels) {
                var childrenEdges = cell2Edge.get(currCell);
                currCell.cost = 1;
                if (childrenEdges != null) {
                    for (var edge : childrenEdges) {
                        long minCost = Integer.MAX_VALUE;
                        Cell minCell = null;

                        for (var cell : edge) {
                            if (cell.cost < minCost) {
                                minCell = cell;
                                minCost = cell.cost;
                            }
                        }
                        edge.minCell = minCell;
                        currCell.cost += minCost;
                    }
                }
            }
        }

        Queue<Cell> cellsToVisit = new LinkedList<>();
        cellsToVisit.add(exampleDelete);
        HashSet<Cell> toDelete = new HashSet<>();
        toDelete.add(exampleDelete);

        while (!cellsToVisit.isEmpty()) {
            var currCell = cellsToVisit.poll();
            var edges = cell2Edge.get(currCell);
            if (edges != null) {
                for (var edge : cell2Edge.get(currCell)) {
                    toDelete.add(edge.minCell);
                    cellsToVisit.add(edge.minCell);
                }
            }
        }
        Utils.optimalDecisionTime += System.nanoTime() - stop;
        Utils.optimalInstantiatedCells += instantiatedCells.size();

        return toDelete;
    }

    private static HashSet<Cell> batchedOptimalDelete(ArrayList<Cell> deletedCells, Instatiator instatiator) throws Exception {
        HashMap<Cell, HashSet<HyperEdge>> cell2Edge = new HashMap<>();
        HashMap<Cell, HashSet<Cell>> cell2Parents = new HashMap<>();
        HashMap<Cell, Cell> cell2Identity = new HashMap<>();

        for (var deleted : deletedCells) {
            if (!cell2Parents.containsKey(deleted)) {
                LinkedList<HashSet<Cell>> treeLevels = new LinkedList<>();
                HashSet<Cell> nextLevel = new HashSet<>();
                HashSet<Cell> currLevel = new HashSet<>();
                cell2Parents.put(deleted, new HashSet<>(0));
                cell2Identity.put(deleted, deleted);
                currLevel.add(deleted);

                while (!currLevel.isEmpty()) {
                    for (var curr : currLevel) {
                        var result = instatiator.instantiateAttachedCells(curr, deleted.insertionTime);
                        Utils.optimalNumEdges += result.size();
                        for (var edge : result) {
                            if (!containsParent(edge, cell2Parents.get(curr))) {
                                var cellIter = edge.iterator();
                                var newCells = new ArrayList<Cell>(edge.size());
                                while (cellIter.hasNext()) {
                                    var cell = cellIter.next();
                                    var unifiedCell = cell2Identity.get(cell);
                                    if (unifiedCell == null) {
                                        cell2Identity.put(cell, cell);
                                        unifiedCell = cell;
                                    } else {
                                        cellIter.remove();
                                        newCells.add(unifiedCell);
                                    }
//                                    if (!cell2Parents.containsKey(unifiedCell)) {
                                        cell2Parents.computeIfAbsent(unifiedCell, a -> new HashSet<>()).add(curr);
                                        nextLevel.add(unifiedCell);
//                                    }
                                }
                                edge.addAll(newCells);
                                cell2Edge.computeIfAbsent(curr, a -> new HashSet<>()).add(edge);
                            }
                        }
                    }
                    treeLevels.addFirst(currLevel);
                    currLevel = nextLevel;
                    nextLevel = new HashSet<>();
                }

                for (var currLevels : treeLevels) {
                    for (var currCell : currLevels) {
                        var childrenEdges = cell2Edge.get(currCell);
                        currCell.cost = 1;
                        if (childrenEdges != null) {
                            for (var edge : childrenEdges) {
                                long minCost = Integer.MAX_VALUE;
                                Cell minCell = null;

                                for (var cell : edge) {
                                    if (cell.cost < minCost) {
                                        minCell = cell;
                                        minCost = cell.cost;
                                    }
                                }
                                edge.minCell = minCell;
                                currCell.cost += minCost;
                            }
                        }
                    }
                }
            }
        }

        for (var deleted : deletedCells) {
            var identityCell = cell2Identity.get(deleted);
            var parents = cell2Parents.get(identityCell);
            for (var parent : parents) {
                for (var edge : cell2Edge.get(parent)) {
                    if (edge.contains(identityCell)) {
                        edge.minCell = identityCell;
                    }
                }
            }
        }

        HashSet<Cell> toDelete = new HashSet<>();
        for (var deleted : deletedCells) {
            Queue<Cell> cellsToVisit = new LinkedList<>();
            cellsToVisit.add(deleted);

            while (!cellsToVisit.isEmpty()) {
                var currCell = cellsToVisit.poll();

                if (toDelete.add(currCell)) {
                    var edges = cell2Edge.get(currCell);
                    if (edges != null) {
                        for (var edge : cell2Edge.get(currCell)) {
                            cellsToVisit.add(edge.minCell);
                        }
                    }
                }
            }
        }
        Utils.optimalInstantiatedCells += cell2Parents.size();

        return toDelete;
    }

    private static HashSet<Cell> ilpApproach(Cell deleted, Instatiator instatiator) throws SQLException, GRBException {
        var instantiatedCells = new HashSet<Cell>();
        Queue<Cell> cellsToVisit = new LinkedList<>();
        HashMap<Cell, ArrayList<HyperEdge>> cell2Edge = new HashMap<>();
        HashMap<Cell, Integer> cell2Id = new HashMap<>();
        int maxId = 0;
        HashMap<Cell, HashSet<Cell>> cell2Parents = new HashMap<>();
        cell2Parents.put(deleted, new HashSet<>(0));
        cellsToVisit.add(deleted);
        var toDelete = new HashSet<Cell>();

        GRBModel model = new GRBModel(env);
        GRBLinExpr obj = new GRBLinExpr();
        model.addVar(1, 1, 0, GRB.BINARY, "a0");
        cell2Id.put(deleted, maxId++);

        int edgeCounter = -1;

        while (!cellsToVisit.isEmpty()) {
            var curr = cellsToVisit.poll();
            instantiatedCells.add(curr);
            int currId = cell2Id.get(curr);

            model.update();
            GRBVar aj = model.getVarByName("a" + currId);
            obj.addTerm(1, aj);
            var result = instatiator.instantiateAttachedCells(curr, deleted.insertionTime);

            for (var edge : result) {
                if (!containsParent(edge, cell2Parents.get(curr))) {
                    var bi = model.addVar(0, 1, 0, GRB.BINARY, "b" + ++edgeCounter);
                    var hij = model.addVar(0, 1, 0, GRB.BINARY, "h" + edgeCounter + currId);
                    model.addConstr(aj, GRB.EQUAL, hij, "");
                    model.addConstr(bi, GRB.EQUAL, hij, "");
                    var tjiVars = new ArrayList<GRBVar>(edge.size());
                    for (var cell : edge) {
                        int tId;
                        GRBVar aCell;
                        if (cell2Id.get(cell) == null) {
                            tId = maxId;
                            cell2Id.put(cell, maxId++);
                            aCell = model.addVar(0, 1, 0, GRB.BINARY, "a" + tId);
                        } else {
                            tId = cell2Id.get(cell);
                            model.update();
                            aCell = model.getVarByName("a" + tId);
                        }

                        var tji = model.addVar(0, 1, 0, GRB.BINARY, "t" + edgeCounter + tId);
                        tjiVars.add(tji);
                        model.addConstr(tji, GRB.EQUAL, aCell, "");
                        cell2Parents.computeIfAbsent(cell, a -> new HashSet<>()).add(curr);
                        if (!instantiatedCells.contains(cell)) {
                            cellsToVisit.add(cell);
                        }
                    }
                    GRBLinExpr tjis = new GRBLinExpr();
                    for (var tji : tjiVars) {
                        tjis.addTerm(1, tji);
                    }
                    model.addConstr(tjis, GRB.GREATER_EQUAL, bi, "");
                    cell2Edge.computeIfAbsent(curr, a -> new ArrayList<>()).add(edge);
                }
            }
        }

        model.setObjective(obj, GRB.MINIMIZE);
        model.update();
        model.optimize();

        if (model.get(GRB.IntAttr.Status) == 3) {
            throw new GRBException("Infeasible model");
        }

        for (var cellEntry : cell2Id.entrySet()) {
            if (model.getVarByName("a" + cellEntry.getValue()).get(GRB.DoubleAttr.X) == 1d) {
                toDelete.add(cellEntry.getKey());
            }
        }

        Utils.ilpInstantiatedCells += cell2Id.size();

        model.dispose();
        return toDelete;
    }

    private static HashSet<Cell> batchedIlpApproach(ArrayList<Cell> deletedCells, Instatiator instatiator, boolean useEarliest) throws SQLException, GRBException {
        long earliestTime = Long.MAX_VALUE;
        for (var deleted : deletedCells) {
            if(deleted.insertionTime < earliestTime) {
                earliestTime = deleted.insertionTime;
            }
        }

        var instantiatedCells = new HashSet<Cell>();
        Queue<Cell> cellsToVisit = new LinkedList<>();
        HashMap<Cell, ArrayList<HyperEdge>> cell2Edge = new HashMap<>();
        HashMap<Cell, Integer> cell2Id = new HashMap<>();
        int maxId = 0;
        int edgeCounter = -1;
        HashMap<Cell, HashSet<Cell>> cell2Parents = new HashMap<>();
        var toDelete = new HashSet<Cell>();
        GRBModel model = new GRBModel(env);
        GRBLinExpr obj = new GRBLinExpr();

        for (var deleted : deletedCells) {
            var localInstantiatedCells = new HashSet<Cell>();
            if (useEarliest && instantiatedCells.contains(deleted)) {
                int tId = cell2Id.get(deleted);
                model.update();
                var aCell = model.getVarByName("a" + tId);
                aCell.set(GRB.DoubleAttr.LB, 1);
            } else {
                if (instantiatedCells.contains(deleted)) {
                    int tId = cell2Id.get(deleted);
                    model.update();
                    var aCell = model.getVarByName("a" + tId);
                    aCell.set(GRB.DoubleAttr.LB, 1);
                } else {
                    cell2Parents.put(deleted, new HashSet<>(0));
                    int initialTId = maxId;
                    cell2Id.put(deleted, maxId++);
                    model.addVar(1, 1, 0, GRB.BINARY, "a" + initialTId);
                }
                cellsToVisit.add(deleted);

                while (!cellsToVisit.isEmpty()) {
                    var curr = cellsToVisit.poll();
                    if (localInstantiatedCells.add(curr) || (useEarliest && !instantiatedCells.contains(curr))) {
                        instantiatedCells.add(curr);
                        int currId = cell2Id.get(curr);

                        model.update();
                        GRBVar aj = model.getVarByName("a" + currId);
                        obj.addTerm(1, aj);
                        var result = instatiator.instantiateAttachedCells(curr, useEarliest ? earliestTime : deleted.insertionTime);

                        for (var edge : result) {
                            if (!containsParent(edge, cell2Parents.get(curr))) {
                                var bi = model.addVar(0, 1, 0, GRB.BINARY, "b" + ++edgeCounter);
                                var hij = model.addVar(0, 1, 0, GRB.BINARY, "h" + edgeCounter + currId);
                                model.addConstr(aj, GRB.EQUAL, hij, "");
                                model.addConstr(bi, GRB.EQUAL, hij, "");
                                var tjiVars = new ArrayList<GRBVar>(edge.size());
                                for (var cell : edge) {
                                    int tId;
                                    GRBVar aCell;
                                    if (cell2Id.get(cell) == null) {
                                        tId = maxId;
                                        cell2Id.put(cell, maxId++);
                                        aCell = model.addVar(0, 1, 0, GRB.BINARY, "a" + tId);
                                    } else {
                                        tId = cell2Id.get(cell);
                                        model.update();
                                        aCell = model.getVarByName("a" + tId);
                                    }

                                    var tji = model.addVar(0, 1, 0, GRB.BINARY, "t" + edgeCounter + tId);
                                    tjiVars.add(tji);
                                    model.addConstr(tji, GRB.EQUAL, aCell, "");
                                    cell2Parents.computeIfAbsent(cell, a -> new HashSet<>()).add(curr);
                                    if (!localInstantiatedCells.contains(cell)) {
                                        cellsToVisit.add(cell);
                                    }
                                }
                                GRBLinExpr tjis = new GRBLinExpr();
                                for (var tji : tjiVars) {
                                    tjis.addTerm(1, tji);
                                }
                                model.addConstr(tjis, GRB.GREATER_EQUAL, bi, "");
                                cell2Edge.computeIfAbsent(curr, a -> new ArrayList<>()).add(edge);
                            }
                        }
                    }
                }
            }
        }

        model.setObjective(obj, GRB.MINIMIZE);
        model.update();
        model.optimize();

        if (model.get(GRB.IntAttr.Status) == 3) {
            throw new GRBException("Infeasible model");
        }
        var cellIter = instantiatedCells.iterator();
        for (int cellIdx = 0; cellIdx < instantiatedCells.size(); cellIdx++) {
            var cell = cellIter.next();
            if (model.getVarByName("a" + cellIdx).get(GRB.DoubleAttr.X) == 1d) {
                toDelete.add(cell);
            }
        }

        model.dispose();
        return toDelete;
    }

    private static void playGround(Cell exampleDelete, Instatiator instatiator) throws SQLException {
        var instantiatedCells = new HashSet<Cell>();
        Queue<Cell> cellsToVisit = new LinkedList<>();
        HashMap<Cell, ArrayList<HyperEdge>> cell2Edge = new HashMap<>();
        cellsToVisit.add(exampleDelete);

        while (!cellsToVisit.isEmpty()) {
            var curr = cellsToVisit.poll();
            instantiatedCells.add(curr);
            var result = instatiator.instantiateAttachedCells(curr, 0);

            var resultIter = result.iterator();
            while (resultIter.hasNext()) {
                var edge = resultIter.next();
                var edgeIter = edge.iterator();
                while (edgeIter.hasNext()) {
                    var cell = edgeIter.next();
                    if (!instantiatedCells.contains(cell)) {
                        cellsToVisit.add(cell);
                    } else {
                        edgeIter.remove();
                    }
                }
                if (edge.isEmpty()) {
                    resultIter.remove();
                }
            }
            if (!result.isEmpty()) {
                cell2Edge.put(curr, result);
            }
        }
        System.out.println(cell2Edge.size());

        ArrayList<Cell> currentLevel = new ArrayList<>();
        ArrayList<Cell> nextLevel;
        currentLevel.add(exampleDelete);

        while (!currentLevel.isEmpty()) {
            nextLevel = new ArrayList<>();
            var edgesOfLevel = new ArrayList<HyperEdge>();
            for (var cell : currentLevel) {
                var edges = cell2Edge.get(cell);
                if (edges != null) {
                    edgesOfLevel.addAll(edges);
                    for (var edge : edges) {
                        nextLevel.addAll(edge);
                    }
                }
            }
            System.out.println(edgesOfLevel);
            System.out.println(nextLevel);
            currentLevel = nextLevel;
        }
        System.out.println(currentLevel);
    }

    private static HashSet<Cell> approximateDelete(Cell exampleDelete, Instatiator instatiator) throws Exception {
        var instantiatedCells = new HashSet<Cell>();
        long sourceInsertionTime = exampleDelete.insertionTime;
        ArrayList<HyperEdge> currentLevel = instatiator.instantiateAttachedCells(exampleDelete, sourceInsertionTime);
        ArrayList<HyperEdge> nextLevel;
        LinkedHashSet<Cell> toDelete = new LinkedHashSet<>();
        instantiatedCells.add(exampleDelete);
        toDelete.add(exampleDelete);
        HashMap<Cell, HashSet<Cell>> cell2Parents = new HashMap<>();
        cell2Parents.put(exampleDelete, new HashSet<>(0));

        for (var edge : currentLevel) {
            for (var cell : edge) {
                cell2Parents.computeIfAbsent(cell, a -> new HashSet<>()).add(exampleDelete);
            }
        }

        while (!currentLevel.isEmpty()) {
            Utils.approximateTraversionDepth++;
            nextLevel = new ArrayList<>();
            for (var edge : currentLevel) {
                if (edge.size() == 1) {
                    var cell = edge.iterator().next();
                    if (instantiatedCells.add(cell)) {
                        toDelete.add(cell);
                        var edges = instatiator.instantiateAttachedCells(cell, sourceInsertionTime);
                        Utils.approximateNumEdges += edges.size();
                        for (var child : edges) {
                            if (!containsParent(child, cell2Parents.get(cell))) {
                                for (var childCell : child) {
                                    cell2Parents.computeIfAbsent(childCell, a -> new HashSet<>()).add(cell);
                                }
                                nextLevel.add(child);
                            }
                        }
                    }
                } else {
                    HashMap<Cell, ArrayList<HyperEdge>> possibleNextEdges = new HashMap<>(edge.size(), 1f);
                    for (var cell : edge) {
                        if (instantiatedCells.add(cell)) {
                            var edges = instatiator.instantiateAttachedCells(cell, sourceInsertionTime);
                            Utils.approximateNumEdges += edges.size();
                            ArrayList<HyperEdge> grandchildren = new ArrayList<>(edges.size() - 1);
                            for (var child : edges) {
                                if (!containsParent(child, cell2Parents.get(cell))) {
                                    for (var childCell : child) {
                                        cell2Parents.computeIfAbsent(childCell, a -> new HashSet<>()).add(cell);
                                    }
                                    grandchildren.add(child);
                                }
                            }
                            possibleNextEdges.put(cell, grandchildren);
                        } else {
                            possibleNextEdges.put(cell, new ArrayList<>(0));
                        }
                    }
                    Cell minCell = null;
                    ArrayList<HyperEdge> minEdges = null;
                    for (var entry : possibleNextEdges.entrySet()) {
                        if (minEdges == null || entry.getValue().size() < minEdges.size()) {
                            minCell = entry.getKey();
                            minEdges = entry.getValue();
                        }
                    }
//                    for (int cellIdx = 0; cellIdx < edge.size(); cellIdx++) {
//                        var possibleEdgeSets = possibleNextEdges.get(cellIdx);
//                        if (possibleEdgeSets.size() < minEdgeCount) {
//                            minEdgeCount = possibleEdgeSets.size();
//                            minCell = cellIdx;
//                        }
//                    }
                    nextLevel.addAll(minEdges);
                    toDelete.add(minCell);
                }
            }
            currentLevel = nextLevel;
        }

        Utils.approximateInstantiatedCells += instantiatedCells.size();

        return toDelete;
    }



    private static ArrayList<HyperEdge> getFilteredEdges(Instatiator instatiator, HashSet<Cell> instantiatedCells, Cell cell, long sourceInsertionTime) throws SQLException {
        instantiatedCells.add(cell);
        var result = instatiator.instantiateAttachedCells(cell, sourceInsertionTime);
        Utils.approximateNumEdges += result.size();
        var resultIter = result.iterator();
        while (resultIter.hasNext()) {
            var nextEdge = resultIter.next();
            nextEdge.removeIf(instantiatedCells::contains);
            if (nextEdge.isEmpty()) {
                resultIter.remove();
            }
        }
        return result;
    }

    private static void parseSchema(String basePath) throws IOException {
        var parser = CSVFormat.DEFAULT.parse(Files.newBufferedReader(Paths.get(basePath, "schema_" + dataset + ".csv")));
        for (var record : parser) {
            tableName2keyCol.put(record.get(0), record.get(1));
        }
    }


    private static void parseRules(String basePath) throws Exception {
        var parser = CSVFormat.DEFAULT.parse(Files.newBufferedReader(Paths.get(basePath, "rules_" + dataset + ".csv")));
        for (var record : parser) {
            var rule = parseRule(record);
            rules.add(rule);
            attributeInHead.computeIfAbsent(rule.head, a -> new ArrayList<>()).add(rule);
            for (var tail : rule.tail) {
                attributeInTail.computeIfAbsent(tail, attribute -> new ArrayList<>()).add(rule);
            }
        }
    }

    private static Rule parseRule(CSVRecord record) throws Exception {
        int keywordsFound = 0;
        Rule rule = new Rule();
        String headString = "";
        ArrayList<String> tailStrings = new ArrayList<>();
        for (String field : record) {
            switch (keywordsFound) {
                case 0: {
                    headString = field;
                    keywordsFound++;
                    break;
                }
                case 1: {
                    if (field.equalsIgnoreCase("from")) {
                        keywordsFound++;
                    } else {
                        tailStrings.add(field);
                    }
                    break;
                }
                case 2: {
                    if (field.equalsIgnoreCase("where")) {
                        keywordsFound++;
                    } else {
                        var tab2Alias = field.split(" ");
                        if (tab2Alias.length != 2) {
                            throw new Exception("Wrong table definition");
                        }
                        rule.table2Alias.put(tab2Alias[0], tab2Alias[1]);
                        rule.table2Alias.put(tab2Alias[1], tab2Alias[0]);
                        rule.tables.add(tab2Alias[0]);
                    }
                    break;
                }
                case 3: {
                    rule.condition = field;
                }
            }
        }
        rule.head = parseAttribute(rule.table2Alias, headString);
        for (var tails : tailStrings) {
            rule.tail.add(parseAttribute(rule.table2Alias, tails));
        }
        return rule;
    }

    public static Attribute parseAttribute(HashMap<String, String> table2Alias, String attrString) throws Exception {
        var splitted = attrString.split("\\.");
        if (splitted.length != 2) {
            throw new Exception("Wrong table definition");
        }
        return new Attribute(table2Alias.get(splitted[0]), splitted[1]);
    }
}
