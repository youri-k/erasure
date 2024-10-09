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
    final static ArrayList<HyperEdge> EMPTY_LIST = new ArrayList<>(0);
    static GRBEnv env;

    private static void parseConfigFile(String jsonString) throws Exception {
        JSONObject root = new JSONObject(jsonString);

        if (root.has("dataset")) {
            ConfigParameter.setDataset(root.getString("dataset"));
        }
        if (root.has("configPath")) {
            ConfigParameter.configPath = root.getString("configPath");
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

        checkNonCyclicRules(allAttributes);

        var instatiator = new Instatiator(attributeInHead, attributeInTail, tableName2keyCol);

        if (ConfigParameter.batching) {
            compareBatch(instatiator, allAttributes);
        } else if (ConfigParameter.scheduling) {
            // TODO
        } else {
            iterateAttributes(instatiator, allAttributes);
        }

        var exampleDelete = new Cell(new Attribute("socialgram.profile", "totalLikes"), "921");
//        socialgram.profile totalLikes[1108] => 0
//        var exampleDelete = new Cell(new Attribute("socialgram.profile", "avgQuotes"), "216");
//        var exampleDelete = new Cell(new Attribute("socialgram.posts", "tweetid"), "951680840706048000");
//        var exampleDelete = new Cell(new Attribute("tax.tax", "ChildExemp"), "415701");
//        instatiator.resetSchema();
//        instatiator.completeCell(exampleDelete);
//        var instantiatedModel = new InstantiatedModel(exampleDelete, instatiator);
//
//        optimalDelete(instantiatedModel, exampleDelete);
//        ilpApproach(instantiatedModel, exampleDelete);
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

    private static HashSet<Cell> runDeletionMethod(Cell deleted, InstantiatedModel instantiatedModel, int deletionMethod, long[] countsArray) throws Exception {
        HashSet<Cell> result = null;
        switch (deletionMethod) {
            case 0:
                result = optimalDelete(instantiatedModel, deleted);
                break;
            case 1:
                result = approximateDelete(instantiatedModel, deleted);
                break;
            case 2:
                result = ilpApproach(instantiatedModel, deleted);
                break;
        }
        countsArray[0] += result.size();
        return result;
    }

    private static long deleteCells(Instatiator instatiator, HashSet<Cell> toDelete) throws SQLException {
        var delStart = System.nanoTime();
        for (var cell : toDelete) {
            instatiator.setToNull(cell);
        }
        return System.nanoTime() - delStart;
    }

    private static void iterateAttributes(Instatiator instatiator, Set<Attribute> attributes) throws Exception {
        writeHeader();
        HashSet<Cell>[] deletionSets = new HashSet[3];
        for (var attr : attributes) {
            System.out.print(attr.toString() + ",");
            instatiator.resetSchema();
            var keys = getKeys(instatiator, attr);
            for (var key : keys) {
                instatiator.resetSchema();
                var deletionCell = new Cell(attr, key);
                instatiator.completeCell(deletionCell);
                var instantiatedModel = new InstantiatedModel(deletionCell, instatiator);
                deletionSets[0] = runDeletionMethod(deletionCell, instantiatedModel, 0, Utils.optimalCounts);
                deletionSets[1] = runDeletionMethod(deletionCell, instantiatedModel, 1, Utils.approximateCounts);
                deletionSets[2] = runDeletionMethod(deletionCell, instantiatedModel, 2, Utils.ilpCounts);

                // speed up experiments by only applying deletes once for optimal/ilp
                assert deletionSets[0].size() == deletionSets[2].size();
                var deletionTime = deleteCells(instatiator, deletionSets[2]);
                Utils.optimalTimes[4] += deletionTime;
                Utils.ilpTimes[4] += deletionTime;
                if (deletionSets[0].size() == deletionSets[1].size()) {
                    Utils.approximateTimes[4] += deletionTime;
                } else {
                    instatiator.resetSchema();
                    Utils.approximateTimes[4] += deleteCells(instatiator, deletionSets[1]);
                }
            }
            writeOutput();
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
                InstantiatedModel model = new InstantiatedModel(deletionCell, instatiator);
                batch.add(deletionCell);
                var start = System.nanoTime();
                naiveOptimalResult.addAll(optimalDelete(model, deletionCell));
                naiveOptimalTime += System.nanoTime() - start;
                start = System.nanoTime();
                naiveApproximateResult.addAll(approximateDelete(model, deletionCell));
                naiveApproximateTime += System.nanoTime() - start;
                start = System.nanoTime();
                naiveILPResult.addAll(ilpApproach(model, deletionCell));
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
        // no model construction for approximate version
//        Utils.approximateTimes[2] -= Utils.approximateTimes[1];
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

    private static HashSet<Cell> optimalDelete(InstantiatedModel model, Cell deleted) {
        Utils.optimalCounts[1] += model.instantiationTime.size();
        Utils.optimalCounts[2] += model.treeLevels.size();
        Utils.optimalTimes[2] += model.modelConstructionTime;

        var start = System.nanoTime();
        for (var currLevels : model.treeLevels) {
            for (var currCell : currLevels) {
                Utils.optimalTimes[1] += model.instantiationTime.getOrDefault(currCell, 0L);
                var childrenEdges = model.cell2Edge.get(currCell);
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
            var edges = model.cell2Edge.get(currCell);
            if (edges != null) {
                for (var edge : model.cell2Edge.get(currCell)) {
                    if (toDelete.add(edge.minCell)) {
                        cellsToVisit.add(edge.minCell);
                    }
                }
            }
        }
        Utils.optimalTimes[3] += System.nanoTime() - start;

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

    private static HashSet<Cell> ilpApproach(InstantiatedModel model, Cell deleted) throws GRBException {
        Utils.ilpTimes[2] += model.modelConstructionTime;
        var start = System.nanoTime();
        int maxId = 0;
        int edgeCounter = -1;
        HashMap<Cell, Integer> cell2Id = new HashMap<>();
        HashSet<Cell> instantiatedCells = new HashSet<>();
        HashSet<Cell> toDelete = new HashSet<>();
        Queue<Cell> cellsToVisit = new LinkedList<>();
        GRBModel grbModel = new GRBModel(env);
        GRBLinExpr obj = new GRBLinExpr();
        grbModel.addVar(1, 1, 0, GRB.BINARY, "a0");
        cell2Id.put(deleted, maxId++);
        cellsToVisit.add(deleted);
        instantiatedCells.add(deleted);

        while (!cellsToVisit.isEmpty()) {
            var curr = cellsToVisit.poll();
            int currId = cell2Id.get(curr);
            grbModel.update();
            GRBVar aj = grbModel.getVarByName("a" + currId);
            obj.addTerm(1, aj);
            Utils.ilpTimes[1] += model.instantiationTime.getOrDefault(curr, 0L);

            var edges = model.cell2Edge.get(curr);
            if (edges != null) {
                for (var edge : edges) {
                    var bi = grbModel.addVar(0, 1, 0, GRB.BINARY, "b" + ++edgeCounter);
                    var hij = grbModel.addVar(0, 1, 0, GRB.BINARY, "h" + edgeCounter + currId);
                    grbModel.addConstr(aj, GRB.EQUAL, hij, "");
                    grbModel.addConstr(bi, GRB.EQUAL, hij, "");
                    var tjiVars = new ArrayList<GRBVar>(edge.size());
                    for (var cell : edge) {
                        int tId;
                        GRBVar aCell;
                        if (cell2Id.get(cell) == null) {
                            tId = maxId;
                            cell2Id.put(cell, maxId++);
                            aCell = grbModel.addVar(0, 1, 0, GRB.BINARY, "a" + tId);
                        } else {
                            tId = cell2Id.get(cell);
                            grbModel.update();
                            aCell = grbModel.getVarByName("a" + tId);
                        }

                        var tji = grbModel.addVar(0, 1, 0, GRB.BINARY, "t" + edgeCounter + tId);
                        tjiVars.add(tji);
                        grbModel.addConstr(tji, GRB.EQUAL, aCell, "");
                        if (instantiatedCells.add(cell)) {
                            cellsToVisit.add(cell);
                        }
                    }
                    GRBLinExpr tjis = new GRBLinExpr();
                    for (var tji : tjiVars) {
                        tjis.addTerm(1, tji);
                    }
                    grbModel.addConstr(tjis, GRB.GREATER_EQUAL, bi, "");
                }
            }
        }

        var stop = System.nanoTime();
        Utils.ilpTimes[2] += stop - start;

        grbModel.setObjective(obj, GRB.MINIMIZE);
        grbModel.update();
        grbModel.optimize();

        if (grbModel.get(GRB.IntAttr.Status) == 3) {
            throw new GRBException("Infeasible grbModel");
        }

        for (var cellEntry : cell2Id.entrySet()) {
            if (grbModel.getVarByName("a" + cellEntry.getValue()).get(GRB.DoubleAttr.X) == 1d) {
                toDelete.add(cellEntry.getKey());
            }
        }
        grbModel.dispose();
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

    private static HashSet<Cell> approximateDelete(InstantiatedModel model, Cell deleted) {
        Cell lastCell = null;
        var start = System.nanoTime();
        HashMap<Cell, Long> instantiatedCells = new HashMap<>();
        var toDelete = new HashSet<Cell>();
        toDelete.add(deleted);

        Queue<Cell> cellsToVisit = new LinkedList<>();
        cellsToVisit.add(deleted);
        instantiatedCells.put(deleted, model.instantiationTime.get(deleted));

        while (!cellsToVisit.isEmpty()) {
            var curr = cellsToVisit.poll();
            Utils.approximateTimes[1] += model.instantiationTime.getOrDefault(curr, 0L);

            var edges = model.cell2Edge.get(curr);
            if (edges != null) {
                for (var edge : edges) {
                    Cell minCell = null;
                    for (var cell : edge) {
                        instantiatedCells.put(cell, model.instantiationTime.get(cell));
                        if (minCell == null || model.cell2Edge.getOrDefault(cell, EMPTY_LIST).size() < model.cell2Edge.getOrDefault(minCell, EMPTY_LIST).size()) {
                            minCell = cell;
                        }
                    }
                    if (toDelete.add(minCell)) {
                        lastCell = minCell;
                        cellsToVisit.add(minCell);
                    }
                }
            }
        }

        Utils.approximateTimes[2] += System.nanoTime() - start;
        Utils.approximateCounts[1] += instantiatedCells.size();
        int count = 0;
        for (var level : model.treeLevels) {
            if (level.contains(lastCell)) break;
            count++;
        }
        Utils.approximateCounts[2] += model.treeLevels.size() - count;

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
