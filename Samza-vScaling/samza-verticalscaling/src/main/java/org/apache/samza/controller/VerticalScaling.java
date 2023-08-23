package org.apache.samza.controller;

//import com.sun.org.apache.regexp.internal.RE;
//import javafx.util.Pair;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.samza.config.Config;
import org.apache.samza.controller.vertical.*;
import org.apache.samza.storage.kv.Entry;
import org.json.simple.JSONObject;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import java.util.*;
import java.util.function.DoubleBinaryOperator;

//Under development

public class VerticalScaling extends StreamSwitch {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.samza.controller.LatencyGuarantor.class);

    private long currentTimeIndex;

    private boolean isStarted;
    private Examiner examiner;
    private Map<String, Resource> shrinks = new HashMap<String, Resource>();
    private Map<String, Resource> expands = new HashMap<String, Resource>();
    private ArrayList<String> flyInstance = new ArrayList<>();
    private long startTime = 0;
    private final ResourceChecker resourceChecker;
    private final Thread resourceCheckerThread;
    private final int blockSize;
    private final boolean cpuSwitch;
    private final boolean memSwitch;
    private final boolean processedArrivalRateSwitch;
    private final String cpuAlgorithmn;
    private final int minMemSize = 600;
    private int memorySavingCount = -1;
    private String lastTime = "cpu";
    private Random rnd;

    public VerticalScaling(Config config){
        super(config);
        String jobName = config.get("job.name");
        examiner = new Examiner(config, metricsRetreiveInterval);
        isStarted = false;
        resourceChecker = new ResourceChecker(jobName);
        this.resourceCheckerThread = new Thread(resourceChecker, "Resource Checker Thread");
        currentTimeIndex = 0;

        blockSize = config.getInt("verticalscaling.block.size", 30);
        cpuSwitch = config.getBoolean("verticalscaling.cpu.switch", true);
        memSwitch = config.getBoolean("verticalscaling.mem.switch", true);
        processedArrivalRateSwitch = config.getBoolean("verticalscaling.processed_arrival_rate.switch", true);
        cpuAlgorithmn = config.get("verticalscaling.cpu.algorithm", "default");
    }

    @Override
    public void init(OperatorControllerListener listener, List<String> executors, List<String> substreams) {
        super.init(listener, executors, substreams);
        examiner.init(executorMapping);
        resourceChecker.setListener(listener);
        resourceCheckerThread.start();
    }


    //Return state validity
    private boolean examine(long timeIndex){
        Map<String, Object> metrics = metricsRetriever.retrieveMetrics();
        long index = adjustTimeIndex();
        if(index > currentTimeIndex)
            currentTimeIndex = (index + currentTimeIndex)/2;
        Map<String, Long> substreamArrived =
                (HashMap<String, Long>) (metrics.get("Arrived"));
        Map<String, Long> substreamProcessed =
                (HashMap<String, Long>) (metrics.get("Processed"));
        Map<String, Boolean> substreamValid =
                (HashMap<String,Boolean>)metrics.getOrDefault("Validity", null);
        Map<String, Double> executorServiceRate =
                (HashMap<String, Double>) (metrics.get("ServiceRate"));
        Map<String, Resource> executorResources =
                (HashMap<String, Resource>) (metrics.get("Resources"));
        Map<String, Double> executorMemUsed =
                (HashMap<String, Double>) (metrics.get("MemUsed"));
        if(executorMemUsed == null || executorMemUsed.size() == 0)
            return false;

        Map<String, Double> executorCpuUsage =
                (HashMap<String, Double>) (metrics.get("CpuUsage"));
        Map<String, Double> executorHeapUsed =
                (HashMap<String, Double>) (metrics.get("HeapUsed"));
        Map<String, Long> pgMajFault = (Map<String, Long>) (metrics.get("PGMajFault"));

//        System.out.println("Model, time " + timeIndex  + ", executor heap used: " + executorHeapUsed);
        System.out.println("Model, time " + currentTimeIndex + ", executor pg major fault: " + pgMajFault);

        adjustCpuUsage(executorCpuUsage, executorResources);

        //Memory usage
        //LOG.info("Metrics size arrived size=" + substreamArrived.size() + " processed size=" + substreamProcessed.size() + " valid size=" + substreamValid.size() + " utilization size=" + executorUtilization.size());
        if(examiner.updateState(currentTimeIndex, substreamArrived, substreamProcessed, substreamValid, executorMapping)){
            examiner.updateExecutorState(currentTimeIndex, executorResources, executorMemUsed, executorCpuUsage, pgMajFault);
            examiner.updateModel(currentTimeIndex, executorServiceRate, executorMapping);
//            examiner.updateResourceState();
            return true;
        }
        return false;
    }

    private void adjustCpuUsage(Map<String, Double> cpuUsage, Map<String, Resource> resourceMap){
        for (Map.Entry<String, Double> entry : cpuUsage.entrySet()){
            String node = entry.getKey();
            if(resourceMap.containsKey(node)) {
                int cores = resourceMap.get(node).getVirtualCores();
                cpuUsage.put(node, entry.getValue() / 100.0 * cores);
            }
        }
    }

    private Resource getTargetResource(Examiner examiner, String executor, Integer deltaMem, Integer deltaCpu){
        Integer targetMem = examiner.state.getMemConfig().get(executor) + deltaMem;
        Integer targetCpu = examiner.state.getCPUConfig().get(executor) + deltaCpu;
        return Resource.newInstance(targetMem, targetCpu);
    }

    private long factorial(long n){
        if (n == 0)
            return 1;
        if (n < 0)
            return 0;
        long result = 1;
        for (int i = 1; i <= n ; i ++){
            result *= i;
        }
        return result;
    }

    private double getLatencyMMK(double aRate, double pRate, int core){
        double atop = aRate/pRate;
        double atopc = aRate/pRate/core;
        double pi_0 = 0;
        for (int i = 0; i < core; i ++){
            pi_0 += Math.pow(atop, i)/factorial(i);
        }
        pi_0 += Math.pow(atop, core) / factorial (core) / (1 - atopc);
        pi_0 = 1/pi_0;
        return Math.pow(atop, core) * pi_0 / factorial(core)/Math.pow(1 - atopc, 2)/pRate/core + 1/pRate;
    }

    private String findOptimalNode(Map<String, Double> aRates, Map<String, Double> pRates, Map<String, Integer> cores){
        Double maxDeltaLatency = 0.0;
        String optimalNode = null;
        for(Map.Entry<String, Double> entry : aRates.entrySet()){
            String node = entry.getKey();
            double oldLatency = getLatencyMMK(entry.getValue(), pRates.get(node), cores.get(node));
            double newLatency = getLatencyMMK(entry.getValue(), pRates.get(node), cores.get(node) + 1);
            if((oldLatency - newLatency) * entry.getValue() > maxDeltaLatency){
                optimalNode = node;
            }
        }
        return optimalNode;
    }

    private boolean elasticutor(Examiner examiner){
        Map<String, Double> arrivalRate = examiner.model.getArrivalRate();
        Map<String, Double> processRatePerCpu = examiner.model.getpRatePerCpu();
        int totalCPU = getTotalCPU(examiner);
        Map<String, Integer> initCores = new HashMap<>();
        for(Map.Entry<String, Double> entry : arrivalRate.entrySet()){
            String node = entry.getKey();
            int cores = (int) Math.floor(entry.getValue()/processRatePerCpu.get(node)) + 1;
            initCores.put(node, cores);
            totalCPU -= cores;
        }
        if(totalCPU < 0) {
            System.out.println("No enough total CPU, require more : " + (-totalCPU));
            return false;
        }
        while(totalCPU > 0){
            totalCPU -= 1;
            String node = findOptimalNode(arrivalRate, processRatePerCpu, initCores);
            if (node == null){
                return false;
            }
            int oldCores = initCores.get(node);
            initCores.put(node, oldCores + 1);
        }

        Map<String, Integer> oldCores = examiner.state.getCPUConfig();
        System.out.println("arrival rate: " + arrivalRate);
        System.out.println("new cores: " + initCores);
        System.out.println("old cores: " + oldCores);
        for(Map.Entry<String, Integer> entry : oldCores.entrySet()){
            String node = entry.getKey();
            int oldQuota = entry.getValue();
            int newQuota = initCores.get(node);
            if (oldQuota < newQuota){
                Resource target = getTargetResource(examiner, entry.getKey(), 0, newQuota - oldQuota);
                expands.put(node, target);
            } else if(newQuota < oldQuota){
                Resource target = getTargetResource(examiner, entry.getKey(), 0, newQuota - oldQuota);
                shrinks.put(node, target);
            }
        }
        return true;
    }


    private Map<String, Integer> largestReminder(Map<String, Double> weights, int total){
        System.out.println("weights: " + weights);
        double sumWeights = 0.0;
        for(Double weight : weights.values()){
            sumWeights+=weight;
        }
        double share = total/sumWeights;
        Map<String, Integer> quotas = new HashMap<>();
        Map<String, Double> reminders = new HashMap<>();
        long totalTemp = total;
        for(Map.Entry<String, Double> entry : weights.entrySet()){
            String key = entry.getKey();
            double quotasDouble = entry.getValue() * share;
            int quotaLong = (int) Math.floor(quotasDouble);
            if(quotaLong == 0) {
                quotaLong += 1;
            } else {
                reminders.put(key, quotasDouble - quotaLong);
            }
            quotas.put(key, quotaLong);
            totalTemp -= quotaLong;
        }

        while (totalTemp < 0){
            double largestRatio = 0.0;
            String unlucky = null;
            for(Map.Entry<String, Double> entry : reminders.entrySet()){
                String node = entry.getKey();
                if(quotas.get(node) <= 1)
                    continue;
                double ratio = quotas.get(node) / weights.get(node);
                if (ratio > largestRatio){
                    largestRatio = ratio;
                    unlucky = node;
                }
            }
            if(unlucky == null){
                LOG.info("Error on LargestReminder");
                return null;
            }
            quotas.put(unlucky, quotas.get(unlucky) - 1);
            totalTemp += 1;
        }


        while(totalTemp > 0){
            Double largestReminder = 0.0;
            String lucky = null;
            for(Map.Entry<String, Double> entry : reminders.entrySet()){
                if(entry.getValue() >= largestReminder){
                    largestReminder = entry.getValue();
                    lucky = entry.getKey();
                }
            }
            if (lucky != null){
                quotas.put(lucky, quotas.get(lucky) + 1);
                reminders.remove(lucky);
            } else{
                LOG.info("Wrong Largest Reminder Algorithm");
                return null;
            }
            totalTemp--;
        }
        return quotas;
    }

    private Map<String, Integer> largestLatency(Map<String, Double> weights, int total, Map<String, Double> latencies){
        double sumWeights = 0.0;
        for(Double weight : weights.values()){
            sumWeights+=weight;
        }
        double share = total/sumWeights;
        Map<String, Integer> quotas = new HashMap<>();
        long totalTemp = total;
        for(Map.Entry<String, Double> entry : weights.entrySet()){
            String key = entry.getKey();
            double quotasDouble = entry.getValue() * share;
            int quotaLong = (int) Math.floor(quotasDouble);
            if(quotaLong == 0)
                quotaLong += 1;
            quotas.put(key, quotaLong);
            totalTemp -= quotaLong;
        }

        while (totalTemp < 0){
            double largestRatio = 0.0;
            String unlucky = null;
            for(Map.Entry<String, Double> entry : latencies.entrySet()){
                String node = entry.getKey();
                if(quotas.get(node) <= 1)
                    continue;
                double ratio = quotas.get(node) / weights.get(node);
                if (ratio > largestRatio){
                    largestRatio = ratio;
                    unlucky = node;
                }
            }
            if(unlucky == null){
                LOG.info("Error on LargestReminder");
                return null;
            }
            quotas.put(unlucky, quotas.get(unlucky) - 1);
            totalTemp += 1;
        }

        Map<String, Double> copyLatencies = new HashMap<>();
        copyLatencies.putAll(latencies);
        while(totalTemp > 0){
            Double largestLatencies = 0.0;
            String lucky = null;
            for(Map.Entry<String, Double> entry : copyLatencies.entrySet()){
                if(entry.getValue() >= largestLatencies){
                    largestLatencies = entry.getValue();
                    lucky = entry.getKey();
                }
            }
            if (lucky != null){
                quotas.put(lucky, quotas.get(lucky) + 1);
                copyLatencies.remove(lucky);
            } else{
                LOG.info("Wrong Largest Latency Algorithm");
                return null;
            }
            totalTemp--;
        }
        return quotas;
    }

    private int getTotalCPU(Examiner examiner){
        int total = examiner.state.getTotalCPU();
        if (total == 0) {
            examiner.state.updateTotalResources();
            total = examiner.state.getTotalCPU();
        }
        return total;
    }

    private boolean CPUDiagnose(Examiner examiner){
        Map<String, Double> weights;
        if(processedArrivalRateSwitch)
            weights = examiner.model.getArrivalRateInDelay();
        else
            weights = examiner.model.getArrivalRate();
        System.out.println(weights);
        Map<String, Integer> cpuConfig = examiner.state.getCPUConfig();
        int totalCPU = getTotalCPU(examiner);
        if (totalCPU < cpuConfig.size()){
            LOG.info("Total CPU size is smaller than the number of instances");
            return false;
        }
        if(weights.size() != cpuConfig.size())
            return false;
        Map<String, Integer> cpuQuotas = largestReminder(weights, totalCPU);

//        Map<String, Integer> cpuQuotas = largestLatency(arrivalRate, totalCPU, examiner.model.getInstantDelay());
        boolean flag = false;
        for (Map.Entry<String, Integer> entry : cpuQuotas.entrySet()){
            String executor = entry.getKey();
            int newQuota = entry.getValue();
            int oldQuota = cpuConfig.get(executor);
            if (newQuota > oldQuota){
                flag = true;
                Resource target = getTargetResource(examiner, entry.getKey(), 0, newQuota - oldQuota);
                expands.put(executor, target);
            } else if(newQuota < oldQuota){
                flag = true;
                Resource target = getTargetResource(examiner, entry.getKey(), 0, newQuota - oldQuota);
                shrinks.put(executor, target);
            }
        }
        System.out.println("targets: "+cpuQuotas);
        return flag;
    }

    private void insertToList(List<String> list, String key, Map<String, Double> values, boolean ascending){
        if (list.size() == 0) {
            list.add(key);
        } else{
            int index = list.size();
            if(ascending){
                for(int i = 0; i < list.size(); i ++){
                    if(values.get(key) < values.get(list.get(i))){
                        index = i;
                        break;
                    }
                }
            } else {
                for(int i = 0; i < list.size(); i ++){
                    if(values.get(key) > values.get(list.get(i))){
                        index = i;
                        break;
                    }
                }
            }
            list.add(index, key);
        }
    }

    private boolean memDiagnose(Examiner examiner){
        if (flyInstance.size() > 0)
            checkMemDone(examiner);
        Map<String, Double> latencies = examiner.model.getInstantDelay();
        Map<String, Double> validRates = examiner.model.validRate;
        Map<String, Integer> configMem = examiner.state.getMemConfig();

        ArrayList<String> rich = new ArrayList<>();
        ArrayList<String> poor = new ArrayList<>();
        for(Map.Entry<String, Double> entry : validRates.entrySet()){
            if(flyInstance.contains(entry.getKey()))
                continue;
            if (entry.getValue() >= 0.9) {
                if (configMem.get(entry.getKey()) > minMemSize) {
                    insertToList(rich, entry.getKey(), latencies, true);
                }
            } else {
                insertToList(poor, entry.getKey(), latencies, false);
            }
        }

        System.out.println("Latency: " + latencies);
        System.out.println("Valid Ratios: " + validRates);
        System.out.println("Poor: " + poor);
        System.out.println("Rich: " + rich);

        if(rich.size() == 0 && poor.size() > 1){
            String takerNode = poor.get(0);
            int index = poor.size() - 1;
            String giverNode = poor.get(index);
            while(configMem.get(giverNode) <= minMemSize + blockSize && index > 0){
                index --;
                giverNode = poor.get(index);
            }
            System.out.println("giver: " + giverNode + ", config mem: " + configMem.get(giverNode));
            if(takerNode.equals(giverNode))
                return false;
            Resource resource1 = getTargetResource(examiner, takerNode, blockSize, 0);
            expands.put(takerNode, resource1);
            Resource resource2 = getTargetResource(examiner, giverNode, -blockSize, 0);
            shrinks.put(giverNode, resource2);
            flyInstance.add(takerNode);
        } else if (poor.size() > 0){
            int length = Math.min(poor.size(), rich.size());
            for(int i = 0; i < length; i ++){
                String giverNode = rich.get(i);
                String takerNode = poor.get(i);
                Resource resource1 = getTargetResource(examiner, takerNode, blockSize, 0);
                expands.put(takerNode, resource1);
                Resource resource2 = getTargetResource(examiner, giverNode, -blockSize, 0);
                shrinks.put(giverNode, resource2);
                flyInstance.add(takerNode);
            }

        } else {
            return false;
        }
        return true;
    }

    private void checkMemDone(Examiner examiner){
        ArrayList<String> takers = new ArrayList<>(flyInstance);
        for(String taker: takers) {
            if(!examiner.state.getMemConfig().containsKey(taker))
                continue;
            int configMem = examiner.state.getMemConfig().get(taker);
            double usedMem = examiner.state.getUsedMem(taker);
            System.out.println("Check Mem Done: executor: " + taker + ", config: " + configMem + ", used: " + usedMem);
            if (configMem - usedMem < blockSize)
                flyInstance.remove(taker);
        }
    }

    private boolean memorySaving(Examiner examiner){
        String container2 = "000002";
        if (currentTimeIndex > 6000 && (currentTimeIndex - 6000) / 3000 > memorySavingCount) {
            int configMem = examiner.state.getMemConfig().get(container2);
            int configCore = examiner.state.getCPUConfig().get(container2);
            int newMem = configMem + 400;
            System.out.println("currentTimeIndex: " + currentTimeIndex + ", mem: " + newMem);
            Resource target2 = Resource.newInstance(newMem, configCore);
            expands.put(container2, target2);
            memorySavingCount += 1;
            return true;
        }
        return false;
    }

    private boolean overhead(Examiner examiner){
        String container2 = "000002";
        int[][] params = {
                {1, 4},
                {2, 8},
                {4, 16},
                {8, 32},
                {16, 60}
        };

        for (int[] param : params) {
            Resource targetInit = Resource.newInstance(4 * 1024, 1);
            Map<String, Resource> mapInit = new HashMap<String, Resource>();
            mapInit.put(container2, targetInit);
            resourceChecker.resizeOpen(mapInit);
            resourceChecker.monitorOpen(mapInit);

            Resource target = Resource.newInstance(param[1] * 1024, param[0]);
            Map<String, Resource> mapTmp = new HashMap<String, Resource>();
            mapTmp.put(container2, target);

            long start = System.currentTimeMillis();
            System.out.println(start + ", start to " +param[0] + " cores, " + param[1] + "Gi;");
            resourceChecker.resizeOpen(mapTmp);
            resourceChecker.monitorFileOpen(mapTmp);
            long end = System.currentTimeMillis();
            double duration = (end - start) * 1.0 / 1000;
            String file = "/home/drg/projects/work2/result-ase";
            String result = "to resource combination: " + param[0] + " cores, " + param[1] + "Gi; duration: " + duration + "s";
            outputRes(file, result);
        }

        return false;
    }



    private void outputRes(String fileName, String str){

        try {
            // 创建 BufferedWriter 对象
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true));

            // 写入字符串到文件
            writer.write(str);
            writer.newLine(); // 可选：写入换行符

            // 关闭 writer
            writer.close();

            System.out.println("String appended to file successfully.");
        } catch (IOException e) {
            System.out.println("An error occurred: " + e.getMessage());
        }
    }


    private boolean diagnose(Examiner examiner, long timeIndex) {
        if (cpuAlgorithmn.equals("memorySaving"))
            return memorySaving(examiner);
        if (cpuAlgorithmn.equals("overhead"))
            return overhead(examiner);
        System.out.println("cpu algorithm: " + cpuAlgorithmn);
        System.out.println("last time: " + lastTime);
        if (cpuAlgorithmn.equals("default")) {
            if(lastTime.equals("mem")) {
                if (cpuSwitch && CPUDiagnose(examiner)) {
                    lastTime = "cpu";
                    return true;
                }
                if (memSwitch && memDiagnose(examiner))
                    return true;
            } else {
                if (memSwitch && memDiagnose(examiner)) {
                    lastTime = "mem";
                    return true;
                }
                if (cpuSwitch && CPUDiagnose(examiner))
                    return true;
            }
        } else if (cpuAlgorithmn.equals("elasticutor")){
            if (cpuSwitch && elasticutor(examiner))
                return true;
            if (memSwitch && memDiagnose(examiner))
                return true;
        }
        return false;
    }


    private boolean diagnose_test(Examiner examiner){
        String container2 = "000002";
        String container3 = "000003";
        String container4 = "000004";
        String container5 = "000005";

        if(currentTimeIndex > 6300){
            Resource target2 = Resource.newInstance(900, 1);
            shrinks.put(container2, target2);
            Resource target3 = Resource.newInstance(900, 1);
            shrinks.put(container3, target3);
            Resource target4 = Resource.newInstance(1400, 1);
            shrinks.put(container4, target4);
            Resource target5 = Resource.newInstance(1400, 5);
            expands.put(container5, target5);
        } else if(currentTimeIndex > 13700){
            Resource target2 = Resource.newInstance(900, 2);
            expands.put(container2, target2);
            Resource target3 = Resource.newInstance(900, 2);
            expands.put(container3, target3);
            Resource target4 = Resource.newInstance(1400, 2);
            expands.put(container4, target4);
            Resource target5 = Resource.newInstance(1400, 2);
            shrinks.put(container5, target5);
        }
        return true;
    }



    //Main logic:  examine->diagnose->treatment->sleep
    void work(long timeIndex) {
        LOG.info("Examine...");
        //Examine
        currentTimeIndex = timeIndex;
        boolean stateValidity = examine(timeIndex);

        if(startTime == 0) {
            startTime = 1;
//            initResourcesMain2();
        }
        if (resourceChecker.isSleeping()) {

            //Check is started
            if (!isStarted) {
                LOG.info("Check started...");
                for (int id : examiner.state.substreamStates.keySet()) {
                    if (examiner.state.checkArrivedPositive(id, currentTimeIndex)) {
                        isStarted = true;
                        break;
                    }
                }
            }

//           System.out.println("state: "+stateValidity + ", is start:" + isStarted);
            if (currentTimeIndex > 1250 && stateValidity && isStarted) {
                shrinks.clear();
                expands.clear();
                if (diagnose(examiner, currentTimeIndex)) {
//                if (diagnose_test(examiner)) {
                    resourceChecker.startAdjust(shrinks, expands);
                }

            }
        }
    }

    @Override
    public synchronized void onMigrationExecutorsStopped(){

    }
    @Override
    public void onMigrationCompleted(){
    }

    @Override
    public void onResourceResized(String processorId, String extendedJobModel) {

    }

    @Override
    public void onContainerResized(String processorId, String result) {

    }
}
