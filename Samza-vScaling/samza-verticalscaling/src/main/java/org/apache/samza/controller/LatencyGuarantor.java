package org.apache.samza.controller;

import org.apache.samza.config.Config;
import org.apache.samza.controller.OperatorControllerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

//Under development

public class LatencyGuarantor extends StreamSwitch {
    private static final Logger LOG = LoggerFactory.getLogger(LatencyGuarantor.class);
    private long latencyReq, windowReq; //Window requirment is stored as number of timeslot
    private double l_threshold; // Check instantDelay  < l and longtermDelay < req
    double initialServiceRate; // Initial prediction by user or system on service rate.
    private Prescription pendingPres;
    private Examiner examiner;


    public LatencyGuarantor(Config config){
        super(config);
        latencyReq = config.getLong("streamswitch.requirement.latency", 1000); //Unit: millisecond
        windowReq = config.getLong("streamswitch.requirement.window", 2000) / metricsRetreiveInterval; //Unit: # of time slots
        l_threshold = config.getDouble("streamswitch.system.l", 100); //Unit: millisecond
        initialServiceRate = config.getDouble("streamswitch.system.initialservicerate", 0.2);
        examiner = new Examiner();
        pendingPres = null;
    }

    @Override
    public void init(OperatorControllerListener listener, List<String> executors, List<String> substreams) {
        super.init(listener, executors, substreams);
        examiner.init(executorMapping);
    }
    class Examiner{
        class State {
            private Map<Long, Map<Integer, List<Integer>>> mappings;
            private Map<Integer, Double> executorUtilization;
            class SubstreamState{
                private Map<Long, Long> arrived, completed; //Instead of actual time, use the n-th time point as key
                private long lastValidIndex;    //Last valid state time point
                //For calculate instant delay
                private Map<Long, Long> totalLatency;
                private long arrivedIndex, remainedIndex;
                SubstreamState(){
                    arrived = new HashMap<>();
                    completed = new HashMap<>();
                    totalLatency = new HashMap<>();
                }
            }
            Map<Integer, SubstreamState> substreamStates;
            private long lastValidTimeIndex;
            private long currentTimeIndex;
            private State() {
                currentTimeIndex = 0;
                lastValidTimeIndex = 0;
                executorUtilization = new HashMap<>();
                mappings = new HashMap<>();
                substreamStates = new HashMap<>();
            }

            private int substreamIdFromStringToInt(String partition){
                return Integer.parseInt(partition.substring(partition.indexOf(' ') + 1));
            }
            private int executorIdFromStringToInt(String executor){
                return Integer.parseInt(executor);
            }

            private void init(Map<String, List<String>> executorMapping){
                LOG.info("Initialize time point 0...");
                for (String executor : executorMapping.keySet()) {
                    for (String substream : executorMapping.get(executor)) {
                        SubstreamState substreamState = new SubstreamState();
                        substreamState.arrived.put(0l, 0l);
                        substreamState.completed.put(0l, 0l);
                        substreamState.lastValidIndex = 0l;
                        substreamState.arrivedIndex = 1l;
                        substreamState.remainedIndex = 0l;
                        substreamStates.put(substreamIdFromStringToInt(substream), substreamState);
                    }
                }
            }

            protected long getTimepoint(long timeIndex){
                return timeIndex * metricsRetreiveInterval;
            }

            protected Map<Integer, List<Integer>> getMapping(long n){
                return mappings.getOrDefault(n, null);
            }
            public long getSubstreamArrived(Integer substream, long n){
                long arrived = 0;
                if(substreamStates.containsKey(substream)){
                    arrived = substreamStates.get(substream).arrived.getOrDefault(n, 0l);
                }
                return arrived;
            }
            public long getSubstreamCompleted(Integer substream, long n){
                long completed = 0;
                if(substreamStates.containsKey(substream)){
                    completed = substreamStates.get(substream).completed.getOrDefault(n, 0l);
                }
                return completed;
            }

            private double getExecutorUtilization(Integer executorId){
                return executorUtilization.getOrDefault(executorId, 0.0);
            }


            public long getSubstreamLatency(Integer substream, long timeIndex){
                long latency = 0;
                if(substreamStates.containsKey(substream)){
                    latency = substreamStates.get(substream).totalLatency.getOrDefault(timeIndex, 0l);
                }
                return latency;
            }

            //Calculate the total latency in new slot. Drop useless data during calculation.
            private void calculateSubstreamLatency(Integer substream, long timeIndex){
                //Calculate total latency and # of tuples in current time slots
                SubstreamState substreamState = substreamStates.get(substream);
                long arrivalIndex = substreamState.arrivedIndex;
                long complete = getSubstreamCompleted(substream, timeIndex);
                long lastComplete = getSubstreamCompleted(substream, timeIndex - 1);
                long arrived = getSubstreamArrived(substream, arrivalIndex);
                long lastArrived = getSubstreamArrived(substream, arrivalIndex - 1);
                long totalDelay = 0;
                while(arrivalIndex <= timeIndex && lastArrived < complete){
                    if(arrived > lastComplete){ //Should count into this slot
                        long number = Math.min(complete, arrived) - Math.max(lastComplete, lastArrived);
                        totalDelay += number * (timeIndex - arrivalIndex);
                    }
                    arrivalIndex++;
                    arrived = getSubstreamArrived(substream, arrivalIndex);
                    lastArrived = getSubstreamArrived(substream, arrivalIndex - 1);
                }
                //TODO: find out when to --
                arrivalIndex--;
                substreamState.totalLatency.put(timeIndex, totalDelay);
                if(substreamState.totalLatency.containsKey(timeIndex - windowReq)){
                    substreamState.totalLatency.remove(timeIndex - windowReq);
                }
                if(arrivalIndex > substreamState.arrivedIndex) {
                    substreamState.arrivedIndex = arrivalIndex;
                }
            }

            private void drop(long timeIndex, Map<String, Boolean> substreamValid) {
                long totalSize = 0;

                //Drop arrived
                for(String substream: substreamValid.keySet()) {
                    SubstreamState substreamState = substreamStates.get(substreamIdFromStringToInt(substream));
                    if (substreamValid.get(substream)) {
                        long remainedIndex = substreamState.remainedIndex;
                        while (remainedIndex < substreamState.arrivedIndex - 1 && remainedIndex < timeIndex - windowReq) {
                            substreamState.arrived.remove(remainedIndex);
                            remainedIndex++;
                        }
                        substreamState.remainedIndex = remainedIndex;
                    }
                }
                //Debug
                for (int substream : substreamStates.keySet()) {
                    SubstreamState substreamState = substreamStates.get(substream);
                    totalSize += substreamState.arrivedIndex - substreamState.remainedIndex + 1;
                }

                //Drop completed, mappings. These are fixed window

                //Drop mappings
                List<Long> removeIndex = new LinkedList<>();
                for(long index:mappings.keySet()){
                    if(index < timeIndex - windowReq - 1){
                        removeIndex.add(index);
                    }
                }
                for(long index:removeIndex){
                    mappings.remove(index);
                }
                removeIndex.clear();
                if (checkValidity(substreamValid)) {
                    for (long index = lastValidTimeIndex + 1; index <= timeIndex; index++) {
                        for (String executor : executorMapping.keySet()) {
                            for (String substream : executorMapping.get(executor)) {
                                //Drop completed
                                if (substreamStates.get(substreamIdFromStringToInt(substream)).completed.containsKey(index - windowReq - 1)) {
                                    substreamStates.get(substreamIdFromStringToInt(substream)).completed.remove(index - windowReq - 1);
                                }
                            }
                        }

                    }
                    lastValidTimeIndex = timeIndex;
                }

                LOG.info("Useless state dropped, current arrived size: " + totalSize + " mapping size: " + mappings.size());
            }

            //Only called when time n is valid, also update substreamLastValid
            private void calibrateSubstream(int substream, long timeIndex){
                SubstreamState substreamState = substreamStates.get(substream);
                long n0 = substreamState.lastValidIndex;
                substreamState.lastValidIndex = timeIndex;
                if(n0 < timeIndex - 1) {
                    //LOG.info("Calibrate state for " + substream + " from time=" + n0);
                    long a0 = state.getSubstreamArrived(substream, n0);
                    long c0 = state.getSubstreamCompleted(substream, n0);
                    long a1 = state.getSubstreamArrived(substream, timeIndex);
                    long c1 = state.getSubstreamCompleted(substream, timeIndex);
                    for (long i = n0 + 1; i < timeIndex; i++) {
                        long ai = (a1 - a0) * (i - n0) / (timeIndex - n0) + a0;
                        long ci = (c1 - c0) * (i - n0) / (timeIndex - n0) + c0;
                        substreamState.arrived.put(i, ai);
                        substreamState.completed.put(i, ci);
                    }
                }

                //Calculate total latency here
                for(long index = n0 + 1; index <= timeIndex; index++){
                    calculateSubstreamLatency(substream, index);
                }
            }

            //Calibrate whole state including mappings and utilizations
            private void calibrate(Map<String, Boolean> substreamValid){
                //Calibrate mappings
                if(mappings.containsKey(currentTimeIndex)){
                    for(long t = currentTimeIndex - 1; t >= 0 && t >= currentTimeIndex - windowReq - 1; t--){
                        if(!mappings.containsKey(t)){
                            mappings.put(t, mappings.get(currentTimeIndex));
                        }else{
                            break;
                        }
                    }
                }

                //Calibrate substreams' state
                if(substreamValid == null)return ;
                for(String substream: substreamValid.keySet()){
                    if (substreamValid.get(substream)){
                        calibrateSubstream(substreamIdFromStringToInt(substream), currentTimeIndex);
                    }
                }
            }



            private void insert(long timeIndex, Map<String, Long> substreamArrived,
                                Map<String, Long> substreamProcessed, Map<String, Double> utilization,
                                Map<String, List<String>> executorMapping) { //Normal update
                LOG.info("Debugging, metrics retrieved data, time: " + timeIndex + " substreamArrived: "+ substreamArrived + " substreamProcessed: "+ substreamProcessed + " assignment: " + executorMapping);

                currentTimeIndex = timeIndex;

                HashMap<Integer, List<Integer>> mapping = new HashMap<>();
                for(String executor: executorMapping.keySet()) {
                    List<Integer> partitions = new LinkedList<>();
                    for (String partitionId : executorMapping.get(executor)) {
                        partitions.add(substreamIdFromStringToInt(partitionId));
                    }
                    mapping.put(executorIdFromStringToInt(executor), partitions);
                }
                mappings.put(currentTimeIndex, mapping);

                LOG.info("Current time " + timeIndex);

                for(String substream:substreamArrived.keySet()){
                    substreamStates.get(substreamIdFromStringToInt(substream)).arrived.put(currentTimeIndex, substreamArrived.get(substream));
                }
                for(String substream: substreamProcessed.keySet()){
                    substreamStates.get(substreamIdFromStringToInt(substream)).completed.put(currentTimeIndex, substreamProcessed.get(substream));
                }

                for(String executor: utilization.keySet()){
                    executorUtilization.put(executorIdFromStringToInt(executor), utilization.get(executor));
                }
            }
        }
        //Model here is only a snapshot
        class Model {
            private State state;
            Map<String, Double> substreamArrivalRate, executorArrivalRate, executorServiceRate, executorInstantaneousDelay; //Longterm delay could be calculated from arrival rate and service rate
            private Model(State state){
                substreamArrivalRate = new HashMap<>();
                executorArrivalRate = new HashMap<>();
                executorServiceRate = new HashMap<>();
                executorInstantaneousDelay = new HashMap<>();
                this.state = state;
            }

            // 1 / ( u - n ). Return  1e100 if u <= n
            private double getLongTermDelay(String executorId){
                double arrival = executorArrivalRate.get(executorId);
                double service = executorServiceRate.get(executorId);
                if(arrival < 1e-15)return 0.0;
                if(service < arrival + 1e-15)return 1e100;
                return 1.0/(service - arrival);
            }

            private double calculateSubstreamArrivalRate(String substream, long n0, long n1){
                if(n0 < 0)n0 = 0;
                long time = state.getTimepoint(n1) - state.getTimepoint(n0);
                long totalArrived = state.getSubstreamArrived(state.substreamIdFromStringToInt(substream), n1) - state.getSubstreamArrived(state.substreamIdFromStringToInt(substream), n0);
                if(time > 1e-9)return totalArrived / ((double)time);
                return 0.0;
            }

            // Calculate window service rate of n - beta ~ n (exclude n - beta)
            private double calculateExecutorServiceRate(String executorId, double util, long n){
                //Because Samza's utilization sometimes goes to 0.0, to avoid service rate become NaN and scale in.
                double lastServiceRate = executorServiceRate.getOrDefault(executorId, 0.0);
                //Only update service rate when util > 1%
                if(util < 0.01){
                    return lastServiceRate;
                }
                //Put here to avoid scale-in at the beginning.
                if(lastServiceRate == 0.0){
                    lastServiceRate = initialServiceRate;
                }
                long completed = 0;
                for(int substream: state.getMapping(n).get(state.executorIdFromStringToInt(executorId))){
                    completed += state.getSubstreamCompleted(substream, n) - state.getSubstreamCompleted(substream, n - 1);
                }
                double instantServiceRate = (completed / ((double)state.getTimepoint(n) - state.getTimepoint(n - 1))) / util;
                double decayFactor = 0.875;
                return decayFactor * lastServiceRate + (1 - decayFactor) * instantServiceRate;
            }

            //Window average delay
            private double calculateExecutorInstantaneousDelay(String executorId, long timeIndex){
                long totalDelay = 0;
                long totalCompleted = 0;
                long n0 = timeIndex - windowReq + 1;
                if(n0<1){
                    n0 = 1;
                    LOG.warn("Calculate instant delay index smaller than window size!");
                }
                for(long i = n0; i <= timeIndex; i++){
                    if(state.getMapping(i).containsKey(state.executorIdFromStringToInt(executorId))){
                        for(int substream: state.getMapping(i).get(state.executorIdFromStringToInt(executorId))){
                            totalCompleted += state.getSubstreamCompleted(substream, i) - state.getSubstreamCompleted(substream, i - 1);
                            totalDelay += state.getSubstreamLatency(substream, i);
                        }
                    }
                }
                //In state, latency is count as # of timeslots, need to transfer to real time
                if(totalCompleted > 0) return totalDelay * metricsRetreiveInterval / ((double)totalCompleted);
                return 0;
            }

            //Calculate model snapshot from state
            private void update(long timeIndex, Map<String, List<String>> executorMapping){
                LOG.info("Updating model snapshot, clear old data...");
                substreamArrivalRate.clear();
                executorArrivalRate.clear();
                executorInstantaneousDelay.clear();
                Map<String, Double> utils = new HashMap<>();
                for(String executor: executorMapping.keySet()){
                    double arrivalRate = 0;
                    for(String substream: executorMapping.get(executor)){
                        double t = calculateSubstreamArrivalRate(substream, timeIndex - windowReq, timeIndex);
                        substreamArrivalRate.put(substream, t);
                        arrivalRate += t;
                    }
                    executorArrivalRate.put(executor, arrivalRate);
                    double util = state.getExecutorUtilization(state.executorIdFromStringToInt(executor));
                    utils.put(executor, util);
                    double mu = calculateExecutorServiceRate(executor, util, timeIndex);
                    /*if(util > 0.5 && util <= 1){ //Only update true service rate (capacity when utilization > 50%, so the error will be smaller)
                        mu /= util;
                        executorServiceRate.put(executor, mu);
                    }else if(!executorServiceRate.containsKey(executor) || (util < 0.3 && executorServiceRate.get(executor) < arrivalRate * 1.5))executorServiceRate.put(executor, arrivalRate * 1.5); //Only calculate the service rate when no historical service rate*/

                    executorServiceRate.put(executor, mu);

                    executorInstantaneousDelay.put(executor, calculateExecutorInstantaneousDelay(executor, timeIndex));
                }
                //Debugging
                LOG.info("Debugging, avg utilization: " + utils);
                LOG.info("Debugging, partition arrival rate: " + substreamArrivalRate);
                LOG.info("Debugging, executor avg service rate: " + executorServiceRate);
                LOG.info("Debugging, executor avg delay: " + executorInstantaneousDelay);
            }
        }

        private Model model;
        private State state;
        Examiner(){
            state = new State();
            model = new Model(state);
        }

        private void init(Map<String, List<String>> executorMapping){
            state.init(executorMapping);
        }

        private boolean checkValidity(Map<String, Boolean> substreamValid){
            if(substreamValid == null)return false;

            //Current we don't haven enough time slot to calculate model
            if(state.currentTimeIndex < windowReq){
                LOG.info("Current time slots number is smaller than beta, not valid");
                return false;
            }
            //Substream Metrics Valid
            for(String executor: executorMapping.keySet()) {
                for (String substream : executorMapping.get(executor)) {
                    if (!substreamValid.containsKey(substream) || !substreamValid.get(substream)) {
                        LOG.info(substream + "'s metrics is not valid");
                        return false;
                    }
                }
            }

            //State Valid
            for(String executor: executorMapping.keySet()){
                if(!state.executorUtilization.containsKey(state.executorIdFromStringToInt(executor))){
                    LOG.info("Current state is not valid, because " + executor + " utilization is missing");
                    return false;
                }
            }
            return true;
        }

        private boolean updateState(long timeIndex, Map<String, Long> substreamArrived, Map<String, Long> substreamProcessed, Map<String, Double> executorUtilization, Map<String, Boolean> substreamValid, Map<String, List<String>> executorMapping){
            LOG.info("Updating state...");
            state.insert(timeIndex, substreamArrived, substreamProcessed, executorUtilization, executorMapping);
            state.calibrate(substreamValid);
            state.drop(timeIndex, substreamValid);
            //Debug & Statistics
            HashMap<Integer, Long> arrived = new HashMap<>(), completed = new HashMap<>();
            for(int substream: state.substreamStates.keySet()) {
                arrived.put(substream, state.substreamStates.get(substream).arrived.get(state.currentTimeIndex));
                completed.put(substream, state.substreamStates.get(substream).completed.get(state.currentTimeIndex));
            }
            System.out.println("State, time " + timeIndex  + " , Partition Arrived: " + arrived);
            System.out.println("State, time " + timeIndex  + " , Partition Completed: " + completed);

            if(checkValidity(substreamValid)){
                //state.calculate(timeIndex);
                //state.drop(timeIndex);
                return true;
            }else return false;
        }

        private void updateModel(long timeIndex, Map<String, List<String>> executorMapping){
            LOG.info("Updating Model");
            model.update(timeIndex, executorMapping);

            //Debug & Statistics
            if(true){
                HashMap<String, Double> longtermDelay = new HashMap<>();
                for(String executorId: executorMapping.keySet()){
                    double delay = model.getLongTermDelay(executorId);
                    longtermDelay.put(executorId, delay);
                }
                System.out.println("Model, time " + timeIndex  + " , Arrival Rate: " + model.executorArrivalRate);
                System.out.println("Model, time " + timeIndex  + " , Service Rate: " + model.executorServiceRate);
                System.out.println("Model, time " + timeIndex  + " , Instantaneous Delay: " + model.executorInstantaneousDelay);
                System.out.println("Model, time " + timeIndex  + " , Longterm Delay: " + longtermDelay);
                System.out.println("Model, time " + timeIndex  + " , Partition Arrival Rate: " + model.substreamArrivalRate);
            }
        }
        private Map<String, Double> getInstantDelay(){
            return model.executorInstantaneousDelay;
        }

        private Map<String, Double> getLongtermDelay(){
            HashMap<String, Double> delay = new HashMap<>();
            for(String executor: executorMapping.keySet()){
                delay.put(executor, model.getLongTermDelay(executor));
            }
            return delay;
        }
    }
    class Prescription {
        String source, target;
        List<String> migratingSubstreams;
        Prescription(){
            migratingSubstreams = null;
        }
        Prescription(String source, String target, List<String> migratingSubstreams){
            this.source = source;
            this.target = target;
            this.migratingSubstreams = migratingSubstreams;
        }
        Map<String, List<String>> generateNewSubstreamAssignment(Map<String, List<String>> oldAssignment){
            Map<String, List<String>> newAssignment = new HashMap<>();
            for(String executor: oldAssignment.keySet()){
                List<String> substreams = new ArrayList<>(oldAssignment.get(executor));
                newAssignment.put(executor, substreams);
            }
            if (!newAssignment.containsKey(target)) newAssignment.put(target, new LinkedList<>());
            for (String substream : migratingSubstreams) {
                newAssignment.get(source).remove(substream);
                newAssignment.get(target).add(substream);
            }
            //For scale in
            if (newAssignment.get(source).size() == 0) newAssignment.remove(source);
            return newAssignment;
        }
    }
    //Return state validity
    private boolean examine(long timeIndex){
        Map<String, Object> metrics = metricsRetriever.retrieveMetrics();
        Map<String, Long> substreamArrived =
                (HashMap<String, Long>) (metrics.get("Arrived"));
        Map<String, Long> substreamProcessed =
                (HashMap<String, Long>) (metrics.get("Processed"));
        Map<String, Double> executorUtilization =
                (HashMap<String, Double>) (metrics.get("Utilization"));
        Map<String, Boolean> substreamValid =
                (HashMap<String,Boolean>)metrics.getOrDefault("Validity", null);

        //Memory usage
        LOG.info("Metrics size arrived size=" + substreamArrived.size() + " processed size=" + substreamProcessed.size() + " valid size=" + substreamValid.size() + " utilization size=" + executorUtilization.size());

        if(examiner.updateState(timeIndex, substreamArrived, substreamProcessed, executorUtilization, substreamValid, executorMapping)){
            examiner.updateModel(timeIndex, executorMapping);
            return true;
        }
        return false;
    }

    //Treatment for Samza
    private void treat(Prescription pres){
        if(pres.migratingSubstreams == null){
            LOG.warn("Prescription has nothing, so do no treatment");
            return ;
        }

        pendingPres = pres;
        isMigrating = true;

        LOG.info("Old mapping: " + executorMapping);
        Map<String, List<String>> newAssignment = pres.generateNewSubstreamAssignment(executorMapping);
        LOG.info("Prescription : src: " + pres.source + " , tgt: " + pres.target + " , migrating: " + pres.migratingSubstreams);
        LOG.info("New mapping: " + newAssignment);
        //Scale out
        if (!executorMapping.containsKey(pres.target)) {
            LOG.info("Scale out");
            //For drawing figure
            System.out.println("Migration! Scale out prescription at time: " + examiner.state.currentTimeIndex + " from executor " + pres.source + " to executor " + pres.target);

            listener.scale(newAssignment.size(), newAssignment);
        }
        //Scale in
        else if(executorMapping.get(pres.source).size() == pres.migratingSubstreams.size()) {
            LOG.info("Scale in");
            //For drawing figure
            System.out.println("Migration! Scale in prescription at time: " + examiner.state.currentTimeIndex + " from executor " + pres.source + " to executor " + pres.target);

            listener.scale(newAssignment.size(), newAssignment);
        }
        //Load balance
        else {
            LOG.info("Load balance");
            //For drawing figure
            System.out.println("Migration! Load balance prescription at time: " + examiner.state.currentTimeIndex + " from executor " + pres.source + " to executor " + pres.target);

            listener.remap(newAssignment);
        }
    }

    //Main logic:  examine->diagnose->treatment->sleep
    void work(long timeIndex) {
        LOG.info("Examine...");
        //Examine
        boolean stateValidity = examine(timeIndex);
        LOG.info("Diagnose...");
        if (stateValidity && !isMigrating) {
            //Diagnose
//            Prescription pres = diagnose(examiner);
//            if (pres.migratingSubstreams != null) {
//                //Treatment
//                treat(pres);
//            } else {
//                LOG.info("Nothing to do this time.");
//            }
        } else {
            if (!stateValidity) LOG.info("Current examine data is not valid, need to wait until valid");
            else if (isMigrating) LOG.info("One migration is in process");
            else LOG.info("Too close to last migration");
        }
    }

    @Override
    public synchronized void onMigrationExecutorsStopped(){
        LOG.info("Migration executors stopped, try to acquire lock...");
        updateLock.lock();
        try {
            if (examiner == null) {
                LOG.warn("Examiner haven't been initialized");
            } else if (!isMigrating || pendingPres == null) {
                LOG.warn("There is no pending migration, please checkout");
            } else {
                String migrationType = "migration";
                //Scale in, remove useless information
                if(pendingPres.migratingSubstreams.size() == executorMapping.get(pendingPres.source).size()){
                    examiner.state.executorUtilization.remove(examiner.state.executorIdFromStringToInt(pendingPres.source));
                    examiner.model.executorServiceRate.remove(pendingPres.source);
                    migrationType = "scale-in";
                }
                if(!executorMapping.containsKey(pendingPres.target)){
                    migrationType = "scale-out";
                }
                //For drawing figre
                LOG.info("Migrating " + pendingPres.migratingSubstreams + " from " + pendingPres.source + " to " + pendingPres.target);

                System.out.println("Executors stopped at time " + (System.currentTimeMillis() - startTime)/metricsRetreiveInterval + " : " + migrationType + " from " + pendingPres.source + " to " + pendingPres.target);

                executorMapping = pendingPres.generateNewSubstreamAssignment(executorMapping);
                pendingPres = null;
            }
        }finally {
            updateLock.unlock();
            LOG.info("Mapping changed, unlock");
        }
    }
    @Override
    public void onMigrationCompleted(){
        LOG.info("Migration completed, try to acquire lock...");
        updateLock.lock();
        try {
            LOG.info("Lock acquired, set migrating flag to false");
            System.out.println("Migration completed at time " + (System.currentTimeMillis() - startTime)/metricsRetreiveInterval);
            isMigrating = false;
        }finally {
            updateLock.unlock();
            LOG.info("Migration completed, unlock");
        }
    }

    @Override
    public void onResourceResized(String processorId, String extendedJobModel) {

    }

    @Override
    public void onContainerResized(String processorId, String result) {

    }
}