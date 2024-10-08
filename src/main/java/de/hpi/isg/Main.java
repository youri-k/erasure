package de.hpi.isg;

import de.hpi.isg.RelationalDependencyRules.Rule;
import de.hpi.isg.RelationalDependencyRules.Attribute;
import de.hpi.isg.RelationalDependencyRules.Cell;
import de.hpi.isg.RelationalDependencyRules.Cell.HyperEdge;
import com.gurobi.gurobi.*;
import org.apache.commons.csv.*;
import org.json.JSONObject;

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

    private static void parseConfigFile(String jsonString) throws Exception {
        JSONObject root = new JSONObject(jsonString);

        if (root.has("dataset")) {
            ConfigParameter.setDataset(root.getString("dataset"));
        }
        if (root.has("ruleFile")) {
            ConfigParameter.ruleFile = root.getString("ruleFile");
        }
        if (root.has("schemaFile")) {
            ConfigParameter.schemaFile = root.getString("schemaFile");
        }
        if (root.has("resultFile")) {
            ConfigParameter.resultFile = root.getString("resultFile");
        }
        if (root.has("batching")) {
            ConfigParameter.batching = root.getBoolean("batching");
        }
        if (root.has("scheduling")) {
            ConfigParameter.scheduling = root.getBoolean("scheduling");
        }
        if (root.has("numKeys")) {
            ConfigParameter.numKeys = root.getInt("numKeys");
        }
        if (root.has("connectionUrl")) {
            ConfigParameter.connectionUrl = root.getString("connectionUrl");
        }
        if (root.has("database")) {
            ConfigParameter.database = root.getString("database");
        }
        if (root.has("username")) {
            ConfigParameter.username = root.getString("username");
        }
        if (root.has("password")) {
            ConfigParameter.password = root.getString("password");
        }
    }

    public static void main(String[] args) throws Exception {
        String configFilePath = args.length > 0 ? args[0] : "config.json";
        parseConfigFile(Files.readString(Paths.get(configFilePath)));

        env = new GRBEnv();
        env.set(GRB.IntParam.OutputFlag, 0);
        env.set(GRB.IntParam.LogToConsole, 0);

        parseRules();
        parseSchema();

        var allAttributes = new HashSet<Attribute>();
        allAttributes.addAll(attributeInTail.keySet());
        allAttributes.addAll(attributeInHead.keySet());

//        checkNonCyclicRules(allAttributes);

        var instatiator = new Instatiator(attributeInHead, attributeInTail, tableName2keyCol);

        if (ConfigParameter.batching) {
            compareBatch(instatiator, allAttributes);
        } else if (ConfigParameter.scheduling) {
            // TODO
        } else {
            iterateAttributes(instatiator, allAttributes);
        }

//        var exampleDelete = new Cell(new Attribute("socialgram.profile", "totalLikes"), "721");
//        socialgram.profile totalLikes[1108] => 0
//        var exampleDelete = new Cell(new Attribute("socialgram.profile", "avgQuotes"), "216");
//        var exampleDelete = new Cell(new Attribute("socialgram.posts", "tweetid"), "951680840706048000");
//        var exampleDelete = new Cell(new Attribute("tax.tax", "ChildExemp"), "415701");
//        instatiator.resetSchema();
//        instatiator.completeCell(exampleDelete);
//
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

    private static void runDeletionMethod(Cell deletionCell, Instatiator instatiator, int deletionMethod, long[] timesArray, long[] countsArray) throws Exception {
        instatiator.resetSchema();
//        var instantiatedModel = new InstantiatedModel(deletionCell, instatiator);
//        optimalDelete(deletionCell, instatiator);
        var start = System.nanoTime();
        HashSet<Cell> result = null;
        switch (deletionMethod) {
            case 0:
                result = optimalDelete(deletionCell, instatiator);
                break;
            case 1:
                result = approximateDelete(deletionCell, instatiator);
                break;
            case 2:
                result = ilpApproach(deletionCell, instatiator);
                break;
        }
        var delStart = System.nanoTime();
        setToNull(instatiator, result);
        var stop = System.nanoTime();
        timesArray[0] += stop - start;
        timesArray[4] += stop - delStart;
        countsArray[0] += result.size();
    }

    private static void iterateAttributes(Instatiator instatiator, Set<Attribute> attributes) throws Exception {
        writeHeader();
        for (var attr : attributes) {
            var debugStart = System.currentTimeMillis();
            System.out.print(attr.toString() + ",");
            instatiator.resetSchema();
            var keys = getKeys(instatiator, attr);
            for (var key : keys) {
                instatiator.resetSchema();
                var deletionCell = new Cell(attr, key);
                instatiator.completeCell(deletionCell);
                runDeletionMethod(deletionCell, instatiator, 0, Utils.optimalTimes, Utils.optimalCounts);
                runDeletionMethod(deletionCell, instatiator, 1, Utils.approximateTimes, Utils.approximateCounts);
                runDeletionMethod(deletionCell, instatiator, 2, Utils.ilpTimes, Utils.ilpCounts);
            }

            writeOutput();
            System.out.println(System.currentTimeMillis() - debugStart);
        }

    }

    private static void compareBatch(Instatiator instatiator, Set<Attribute> attributes) throws Exception {
//        writeHeader();
        var batch = new ArrayList<Cell>(ConfigParameter.numKeys * attributes.size());
        var naiveOptimalResult = new HashSet<Cell>();
        var naiveApproximateResult = new HashSet<Cell>();
        var naiveILPResult = new HashSet<Cell>();
        long naiveOptimalTime = 0L, batchOptimalTime = 0L, naiveApproximateTime = 0L, batchApproximateTime = 0L, naiveILPTime = 0L, batchILPTime = 0L;
        for (var attr : attributes) {
            System.out.print(attr.toString() + ",");
            var keys = getKeys(instatiator, attr);
            for (var key : keys) {
                var deletionCell = new Cell(attr, key);
                instatiator.completeCell(deletionCell);
                batch.add(deletionCell);
                var start = System.nanoTime();
                naiveOptimalResult.addAll(optimalDelete(deletionCell, instatiator));
                naiveOptimalTime += System.nanoTime() - start;
                start = System.nanoTime();
                naiveApproximateResult.addAll(approximateDelete(deletionCell, instatiator));
                naiveApproximateTime += System.nanoTime() - start;
                start = System.nanoTime();
                naiveILPResult.addAll(ilpApproach(deletionCell, instatiator));
                naiveILPTime += System.nanoTime() - start;
            }
        }
        System.out.println();
        var start = System.nanoTime();
        var batchedOptimalResult = batchedOptimalDelete(batch, instatiator);
        batchOptimalTime = System.nanoTime() - start;
        start = System.nanoTime();
        var batchedApproximateResult = batchedApproximateDelete(batch, instatiator);
        batchApproximateTime = System.nanoTime() - start;
        start = System.nanoTime();
        var batchedILPResult = batchedIlpApproach(batch, instatiator);
        batchILPTime = System.nanoTime() - start;

        System.out.println(naiveOptimalResult.size() + "," + batchedOptimalResult.size() + "," + (long) (naiveOptimalTime / 1e6) + "," + (long) (batchOptimalTime / 1e6));
        System.out.println(naiveApproximateResult.size() + "," + batchedApproximateResult.size() + "," + (long) (naiveApproximateTime / 1e6) + "," + (long) (batchApproximateTime / 1e6));
        System.out.println(naiveILPResult.size() + "," + batchedILPResult.size() + "," + (long) (naiveILPTime / 1e6) + "," + (long) (batchILPTime / 1e6));
    }

    private static void writeHeader() {
        System.out.println("Attribute,optimalTime,optimalInstantiationTime,optimalModelTime,optimalOptimizationTime,optimalDeletionTime,approximateTime,approximateInstantiationTime,approximateModelTime,approximateOptimizationTime,approximateDeletionTime,ilpTime,ilpInstantiationTime,ilpModelTime,ilpOptimizationTime,ilpDeletionTime,optimalDeletes,optimalInstantiations,optimalHeight,approximateDeletes,approximateInstantiations,approximateHeight,ilpDeletes,ilpInstantiations,ilpHeight");
    }

    private static String getTimeString(long time) {
        return String.valueOf((long) (time / 1e6));
    }

    private static void writeOutput() {
        ArrayList<String> output = new ArrayList<>();
        // subtract instantiation time from model construction
        Utils.optimalTimes[2] -= Utils.optimalTimes[1];
        Utils.approximateTimes[2] -= Utils.approximateTimes[1];
        Utils.ilpTimes[2] -= Utils.ilpTimes[1];
        for (var time : Utils.optimalTimes) {
            output.add(getTimeString(time));
        }
        for (var time : Utils.approximateTimes) {
            output.add(getTimeString(time));
        }
        for (var time : Utils.ilpTimes) {
            output.add(getTimeString(time));
        }
        for (var count : Utils.optimalCounts) {
            output.add(String.valueOf(count));
        }
        for (var count : Utils.approximateCounts) {
            output.add(String.valueOf(count));
        }
        for (var count : Utils.ilpCounts) {
            output.add(String.valueOf(count));
        }
        System.out.println(String.join(",", output));
        Arrays.fill(Utils.optimalTimes, 0L);
        Arrays.fill(Utils.approximateTimes, 0L);
        Arrays.fill(Utils.ilpTimes, 0L);
        Arrays.fill(Utils.optimalCounts, 0L);
        Arrays.fill(Utils.approximateCounts, 0L);
        Arrays.fill(Utils.ilpCounts, 0L);
    }

    private static ArrayList<String> getKeys(Instatiator instatiator, Attribute attr) throws SQLException {
        ArrayList<String> keys = new ArrayList<>(ConfigParameter.numKeys);
        var resultSet = instatiator.statement.executeQuery("SELECT " + tableName2keyCol.get(attr.table) + " FROM " + attr.table + " ORDER BY RANDOM() LIMIT " + ConfigParameter.numKeys);
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

    private static boolean containsParent(HyperEdge edge, Cell parent) {
        for (var cell : edge) {
            if (parent.equals(cell)) {
                return true;
            }
        }
        return false;
    }

    private static void setToNull(Instatiator instatiator, Set<Cell> toDelete) throws SQLException {
        for (var cell : toDelete) {
            instatiator.setToNull(cell);
        }
    }


    private static HashSet<Cell> optimalDelete(Cell deleted, Instatiator instatiator) throws Exception {
        var start = System.nanoTime();
        var instantiatedCells = new HashSet<Cell>();
        LinkedList<HashSet<Cell>> treeLevels = new LinkedList<>();
        HashSet<Cell> nextLevel = new HashSet<>();
        HashSet<Cell> currLevel = new HashSet<>();

        HashMap<Cell, ArrayList<HyperEdge>> cell2Edge = new HashMap<>();
        HashMap<Cell, HashSet<Cell>> cell2Parents = new HashMap<>();
        cell2Parents.put(deleted, new HashSet<>(0));
        currLevel.add(deleted);

        HashMap<Cell, Cell> cell2Identity = new HashMap<>();
        cell2Identity.put(deleted, deleted);

        while (!currLevel.isEmpty()) {
            HashMap<Cell, HashSet<Cell>> localCell2Parents = new HashMap<>();
            for (var curr : currLevel) {
                if (instantiatedCells.add(curr)) {
                    var instantiationStart = System.nanoTime();
                    var result = instatiator.instantiateAttachedCells(curr, deleted.insertionTime);
//                Utils.optimalCounts[2]++;
                    Utils.optimalTimes[1] += System.nanoTime() - instantiationStart;
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
                                localCell2Parents.computeIfAbsent(unifiedCell, a -> new HashSet<>()).add(curr);
                                nextLevel.add(unifiedCell);
                            }
                            edge.addAll(newCells);
                            cell2Edge.computeIfAbsent(curr, a -> new ArrayList<>()).add(edge);
                        }
                    }
                }
            }
            for (var entry : localCell2Parents.entrySet()) {
                cell2Parents.merge(entry.getKey(), entry.getValue(), (a, b) -> {
                    a.addAll(b);
                    return a;
                });
            }
            treeLevels.addFirst(currLevel);
            currLevel = nextLevel;
            nextLevel = new HashSet<>();
        }

        var stop = System.nanoTime();
        Utils.optimalTimes[2] += stop - start;
        Utils.optimalCounts[2] += treeLevels.size();

        for (var currLevels : treeLevels) {
            for (var currCell : currLevels) {
                var childrenEdges = cell2Edge.get(currCell);
                currCell.cost = 1;
                if (childrenEdges != null) {
                    for (var edge : childrenEdges) {
                        long minCost = Integer.MAX_VALUE;
                        Cell minCell = null;

                        for (var cell : edge) {
                            if (minCell == null || cell.cost < minCost) {
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
        cellsToVisit.add(deleted);
        HashSet<Cell> toDelete = new HashSet<>();
        toDelete.add(deleted);

        while (!cellsToVisit.isEmpty()) {
            var currCell = cellsToVisit.poll();
            var edges = cell2Edge.get(currCell);
            if (edges != null) {
                for (var edge : cell2Edge.get(currCell)) {
                    if (toDelete.add(edge.minCell)) {
                        cellsToVisit.add(edge.minCell);
                    }
                }
            }
        }
        Utils.optimalTimes[3] += System.nanoTime() - stop;
        Utils.optimalCounts[1] += instantiatedCells.size();

        return toDelete;
    }

    private static HashSet<Cell> batchedOptimalDelete(ArrayList<Cell> deletedCells, Instatiator instatiator) throws Exception {
        HashMap<Cell, HashSet<HyperEdge>> cell2Edge = new HashMap<>();
        HashMap<Cell, HashSet<Cell>> cell2Parents = new HashMap<>();
        HashMap<Cell, Cell> cell2Identity = new HashMap<>();

        for (var deleted : deletedCells) {
            var localInstantiatedCells = new HashSet<Cell>();
            LinkedList<HashSet<Cell>> treeLevels = new LinkedList<>();
            HashSet<Cell> nextLevel = new HashSet<>();
            HashSet<Cell> currLevel = new HashSet<>();
            cell2Parents.putIfAbsent(deleted, new HashSet<>(0));
            cell2Identity.putIfAbsent(deleted, deleted);
            currLevel.add(deleted);

            while (!currLevel.isEmpty()) {
                for (var curr : currLevel) {
                    if (localInstantiatedCells.add(curr)) {
                        var result = instatiator.instantiateAttachedCells(curr, deleted.insertionTime);
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
                                    if (!localInstantiatedCells.contains(unifiedCell)) {
                                        cell2Parents.computeIfAbsent(unifiedCell, a -> new HashSet<>()).add(curr);
                                        nextLevel.add(unifiedCell);
                                    }
                                }
                                edge.addAll(newCells);
                                cell2Edge.computeIfAbsent(curr, a -> new HashSet<>()).add(edge);
                            }
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

        return toDelete;
    }

    private static HashSet<Cell> ilpApproach(Cell deleted, Instatiator instatiator) throws SQLException, GRBException {
        var start = System.nanoTime();
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
        instantiatedCells.add(deleted);

        int edgeCounter = -1;

        while (!cellsToVisit.isEmpty()) {
            var curr = cellsToVisit.poll();
//            instantiatedCells.add(curr);
            int currId = cell2Id.get(curr);

            model.update();
            GRBVar aj = model.getVarByName("a" + currId);
            obj.addTerm(1, aj);

            var instantiationStart = System.nanoTime();
            var result = instatiator.instantiateAttachedCells(curr, deleted.insertionTime);
//            Utils.ilpCounts[2]++;
            Utils.ilpTimes[1] += System.nanoTime() - instantiationStart;

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
                        if (instantiatedCells.add(cell)) {
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

        var stop = System.nanoTime();
        Utils.ilpTimes[2] += stop - start;

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
        model.dispose();
        Utils.ilpTimes[3] += System.nanoTime() - stop;
        Utils.ilpCounts[1] += cell2Id.size();

        return toDelete;
    }

    private static HashSet<Cell> batchedIlpApproach(ArrayList<Cell> deletedCells, Instatiator instatiator) throws SQLException, GRBException {
        Queue<Cell> cellsToVisit = new LinkedList<>();
        HashMap<Cell, HashSet<HyperEdge>> cell2Edge = new HashMap<>();
        HashMap<Cell, Integer> cell2Id = new HashMap<>();
        int maxId = 0;
        int edgeCounter = -1;
        HashMap<Cell, HashSet<Cell>> cell2Parents = new HashMap<>();
        var toDelete = new HashSet<Cell>();
        GRBModel model = new GRBModel(env);
        GRBLinExpr obj = new GRBLinExpr();

        for (var deleted : deletedCells) {
            var localInstantiatedCells = new HashSet<Cell>();

            if (cell2Id.containsKey(deleted)) {
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
                if (localInstantiatedCells.add(curr)) {
                    int currId = cell2Id.get(curr);

                    model.update();
                    GRBVar aj = model.getVarByName("a" + currId);
                    obj.addTerm(1, aj);
                    var result = instatiator.instantiateAttachedCells(curr, deleted.insertionTime);

                    for (var edge : result) {
                        if (!containsParent(edge, cell2Parents.get(curr))) {
                            var edgeSet = cell2Edge.computeIfAbsent(curr, a -> new HashSet<>());
                            if (edgeSet.add(edge)) {
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

        for (var cellEntry : cell2Id.entrySet()) {
            if (model.getVarByName("a" + cellEntry.getValue()).get(GRB.DoubleAttr.X) == 1d) {
                toDelete.add(cellEntry.getKey());
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

    private static HashSet<Cell> approximateDelete(Cell deleted, Instatiator instatiator) throws Exception {
        var start = System.nanoTime();
        var instantiatedCells = new HashSet<Cell>();
        ArrayList<HyperEdge> currentLevel = instatiator.instantiateAttachedCells(deleted, deleted.insertionTime);
        Utils.approximateCounts[2]++;
        ArrayList<HyperEdge> nextLevel;
        var toDelete = new HashSet<Cell>();
        instantiatedCells.add(deleted);
        toDelete.add(deleted);
        HashMap<Cell, HashSet<Cell>> cell2Parents = new HashMap<>();
        cell2Parents.put(deleted, new HashSet<>(0));

        for (var edge : currentLevel) {
            for (var cell : edge) {
                cell2Parents.computeIfAbsent(cell, a -> new HashSet<>()).add(deleted);
            }
        }

        while (!currentLevel.isEmpty()) {
            HashMap<Cell, HashSet<Cell>> localCell2Parents = new HashMap<>();
            Utils.approximateCounts[2]++;
            nextLevel = new ArrayList<>();
            for (var edge : currentLevel) {
                HashMap<Cell, ArrayList<HyperEdge>> possibleNextEdges = new HashMap<>(edge.size(), 1f);
                for (var cell : edge) {
                    if (instantiatedCells.add(cell)) {
                        var instantiationStart = System.nanoTime();
                        var edges = instatiator.instantiateAttachedCells(cell, deleted.insertionTime);
                        Utils.approximateTimes[1] += System.nanoTime() - instantiationStart;
                        ArrayList<HyperEdge> grandchildren = new ArrayList<>(edges.size() - 1);
                        for (var child : edges) {
                            if (!containsParent(child, cell2Parents.get(cell))) {
                                for (var childCell : child) {
                                    localCell2Parents.computeIfAbsent(childCell, a -> new HashSet<>()).add(cell);
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
                nextLevel.addAll(minEdges);
                toDelete.add(minCell);
            }
            for (var entry : localCell2Parents.entrySet()) {
                cell2Parents.merge(entry.getKey(), entry.getValue(), (a, b) -> {
                    a.addAll(b);
                    return a;
                });
            }
            currentLevel = nextLevel;
        }

        Utils.approximateTimes[2] += System.nanoTime() - start;
        Utils.approximateCounts[1] += instantiatedCells.size();

        return toDelete;
    }

    private static Cell getAndSetUnifiedCell(Cell cell, HashMap<Cell, Cell> cell2Identity) {
        Cell unifiedCell = cell2Identity.get(cell);
        if (unifiedCell == null) {
            cell2Identity.put(cell, cell);
            return cell;
        } else {
            return unifiedCell;
        }
    }


    private static HashSet<Cell> batchedApproximateDelete(ArrayList<Cell> deletedCells, Instatiator instatiator) throws Exception {
        HashMap<Cell, HashSet<HyperEdge>> cell2Edge = new HashMap<>();
        HashMap<Cell, HashSet<Cell>> cell2Parents = new HashMap<>();
        HashMap<Cell, Cell> cell2Identity = new HashMap<>();

        for (var deleted : deletedCells) {
            deleted = getAndSetUnifiedCell(deleted, cell2Identity);
            var localInstantiatedCells = new HashSet<Cell>();
            localInstantiatedCells.add(deleted);

            var initialEdges = instatiator.instantiateAttachedCells(deleted, deleted.insertionTime);
            ArrayList<HyperEdge> newEdges = new ArrayList<>(initialEdges.size());
            for (var edge : initialEdges) {
                var newEdge = new HyperEdge(edge.size());
                for (var cell : edge) {
                    var uniCell = getAndSetUnifiedCell(cell, cell2Identity);
                    newEdge.add(uniCell);
                    cell2Parents.computeIfAbsent(cell, a -> new HashSet<>()).add(deleted);
                }
                newEdges.add(newEdge);
            }

            HashSet<HyperEdge> nextLevel = new HashSet<>();
            HashSet<HyperEdge> currLevel = new HashSet<>(newEdges);
            cell2Parents.putIfAbsent(deleted, new HashSet<>(0));
            cell2Edge.computeIfAbsent(deleted, a -> new HashSet<>()).addAll(currLevel);

            while (!currLevel.isEmpty()) {
                for (var edge : currLevel) {
                    if (edge.size() == 1) {
                        var cell = edge.iterator().next();
                        edge.minCell = cell;
                        if (localInstantiatedCells.add(cell)) {
                            var edges = instatiator.instantiateAttachedCells(cell, deleted.insertionTime);
                            for (var child : edges) {
                                if (!containsParent(child, cell2Parents.get(cell))) {
                                    var newChild = new HyperEdge(child.size());
                                    for (var childCell : child) {
                                        childCell = getAndSetUnifiedCell(childCell, cell2Identity);
                                        cell2Parents.computeIfAbsent(childCell, a -> new HashSet<>()).add(cell);
                                        newChild.add(childCell);
                                    }
                                    cell2Edge.computeIfAbsent(deleted, a -> new HashSet<>()).add(newChild);
                                    nextLevel.add(newChild);
                                }
                            }
                        }
                    } else {
                        HashMap<Cell, HashSet<HyperEdge>> possibleNextEdges = new HashMap<>(edge.size(), 1f);
                        for (var cell : edge) {
                            if (localInstantiatedCells.add(cell)) {
                                var edges = instatiator.instantiateAttachedCells(cell, deleted.insertionTime);
                                HashSet<HyperEdge> grandchildren = new HashSet<>(edges.size(), 1.0f);
                                for (var child : edges) {
                                    if (!containsParent(child, cell2Parents.get(cell))) {
                                        var newChild = new HyperEdge(child.size());
                                        for (var childCell : child) {
                                            childCell = getAndSetUnifiedCell(childCell, cell2Identity);
                                            cell2Parents.computeIfAbsent(childCell, a -> new HashSet<>()).add(cell);
                                            newChild.add(childCell);
                                        }
                                        grandchildren.add(newChild);
                                    }
                                }
                                cell2Edge.computeIfAbsent(cell, a -> new HashSet<>()).addAll(grandchildren);
                                possibleNextEdges.put(cell, grandchildren);
                            } else {
                                var edges = cell2Edge.get(cell);
                                if (edges == null) {
                                    possibleNextEdges.put(cell, new HashSet<>(0));
                                } else {
                                    possibleNextEdges.put(cell, cell2Edge.get(cell));
                                }
                            }
                        }
                        Cell minCell = null;
                        HashSet<HyperEdge> minEdges = null;
                        for (var entry : possibleNextEdges.entrySet()) {
                            if (minEdges == null || entry.getValue().size() < minEdges.size()) {
                                minCell = entry.getKey();
                                minEdges = entry.getValue();
                            }
                        }
                        nextLevel.addAll(minEdges);
                        edge.minCell = minCell;
                    }
                }
                currLevel = nextLevel;
                nextLevel = new HashSet<>();
            }
        }

        for (var deleted : deletedCells) {
            var parents = cell2Parents.get(deleted);
            for (var parent : parents) {
                for (var edge : cell2Edge.get(parent)) {
                    if (edge.contains(deleted)) {
                        edge.minCell = deleted;
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

        return toDelete;
    }

    private static void parseSchema() throws IOException {
        var parser = CSVFormat.DEFAULT.parse(Files.newBufferedReader(Paths.get(ConfigParameter.configPath, ConfigParameter.schemaFile)));
        for (var record : parser) {
            tableName2keyCol.put(record.get(0), record.get(1));
        }
    }


    private static void parseRules() throws Exception {
        var parser = CSVFormat.DEFAULT.parse(Files.newBufferedReader(Paths.get(ConfigParameter.configPath, ConfigParameter.ruleFile)));
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
