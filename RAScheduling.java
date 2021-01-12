package org.apache.storm.scheduler;

import com.google.common.collect.Sets;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.storm.utils.Utils;

public class RAScheduling implements IScheduler {

    private TopologyDetails t;
    private Cluster cluster;

    @Override
    public void prepare(Map<String, Object> conf) {//noop
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        this.cluster = cluster;
        Collection<TopologyDetails> topologyDetails = topologies.getTopologies();
        for (TopologyDetails t : topologyDetails) {
            this.t = t;
            Map<WorkerSlot, List<ExecutorDetails>> aliveAssigned = EvenScheduler.getAliveAssignedWorkerSlotExecutors(cluster, t.getId());
            Set<ExecutorDetails> aliveExecutors = new HashSet<ExecutorDetails>();
            for (List<ExecutorDetails> list : aliveAssigned.values()) {
                aliveExecutors.addAll(list);
            }
            int NumWorker = t.getNumWorkers();
            Set<ExecutorDetails> allExecutors = t.getExecutors();
            List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
            Set<ExecutorDetails> reassignExecutors = Sets.difference(allExecutors, aliveExecutors);
            Map<ExecutorDetails, WorkerSlot> slotMap = MapExecutorToSlot(t.getId(), reassignExecutors, NumWorker, availableSlots);
            AllocateSlotsToNodes(slotMap);
        }
    }

    @Override
    public Map<String, Map<String, Double>> config() {
        return new HashMap<>();
    }

    public Map<ExecutorDetails, WorkerSlot> MapExecutorToSlot(String t, Set<ExecutorDetails> allExecutors, int NumWorker, List<WorkerSlot> availableSlots) {
        HashMap<WorkerSlot, Set<ExecutorDetails>> slotMap = new HashMap<>();
        int AssignedSlots = 0, AssignedExecutors = 0;
        int TotalExecutors = allExecutors.size();
        List<ExecutorDetails> executors = new ArrayList<ExecutorDetails>(allExecutors);
        Map<ExecutorDetails, WorkerSlot> assignment = new HashMap<ExecutorDetails, WorkerSlot>();
        ArrayList trafficList = new ArrayList();
        try {
            trafficList = ReadFromYaml();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            System.out.println("Line # 65");
        }

        int MaxExecutorPerSlot = (int) Math.ceil(TotalExecutors / NumWorker);
        if (NumWorker == 1) {
            for (int i = 0; i < availableSlots.size(); i++) {
                assignment.put(executors.get(i), availableSlots.get(0));
            }
            //AssignedExecutors += TotalExecutors;
            Map<WorkerSlot, List<ExecutorDetails>> nodePortToExecutors = Utils.reverseMap(assignment);
            for (Map.Entry<WorkerSlot, List<ExecutorDetails>> entry : nodePortToExecutors.entrySet()) {
                WorkerSlot nodePort = entry.getKey();
                List<ExecutorDetails> executor = entry.getValue();
                cluster.assign(nodePort, t, executor);
            }
        } else if (NumWorker >= TotalExecutors) {
            for (int i = 0; i < availableSlots.size(); i++) {
                assignment.put(executors.get(i), availableSlots.get(i));
                //AssignedExecutors++;
                //AssignedSlots++;
            }
            Map<WorkerSlot, List<ExecutorDetails>> nodePortToExecutors = Utils.reverseMap(assignment);
            for (Map.Entry<WorkerSlot, List<ExecutorDetails>> entry : nodePortToExecutors.entrySet()) {
                WorkerSlot nodePort = entry.getKey();
                List<ExecutorDetails> executor = entry.getValue();
                cluster.assign(nodePort, t, executor);
            }
        } else {

            int temp;
            String[] tem = new String[1];
            for (int j = 0; j < trafficList.size(); j++) {
                String[] x = (String[]) trafficList.get(0);
                temp = Integer.parseInt(x[2]);
                for (int i = j + 1; i < trafficList.size(); i++) {
                    String[] c = (String[]) trafficList.get(i);
                    if (temp > Integer.parseInt(c[2])) {
                        tem = (String[]) trafficList.get(i);
                        trafficList.set(i, trafficList.get(j));
                        trafficList.set(j, tem);
                    }
                }
            }

            System.out.println("6");
            ArrayList resourceList = null;
            int count = 0;
            try {
                resourceList = ReadResourceFromYaml();
            } catch (Exception ex) {
                System.out.println("Line # 111");
                System.out.println(ex.getMessage());
            }

            Map<ExecutorDetails, String> map = this.t.getExecutorToComponent();
            Set<ExecutorDetails> executorDetailsSet = map.keySet();

            List<WorkerSlot> availableSlotsList = cluster.getAvailableSlots();

            for (int i = 0; i < trafficList.size(); i++) {
                System.out.println("6.2");
                String[] executor = (String[]) trafficList.get(i);

                for (ExecutorDetails executorDetails : executorDetailsSet) {

                    if (executor[0].equals(executorDetails)) {
                        System.out.println("6.3");
                        for (int k = 0; k < resourceList.size(); k++) {
                            String[] resource = (String[]) resourceList.get(k);

                            for (int j = 0; j < availableSlotsList.size(); j++) {
                                System.out.println("6.4");
                                String nodeId = availableSlotsList.get(j).getNodeId();

                                if (nodeId.equalsIgnoreCase(resource[0])) {
                                    assignment.put(executorDetails, availableSlotsList.get(j));
                                    AssignedExecutors++;
                                    AssignedSlots++;
                                    count++;

                                    System.out.println("6.5");
                                    // To Do : this should be while loop in future
                                    if (count < MaxExecutorPerSlot) {
                                        for (ExecutorDetails executorDetails1 : executorDetailsSet) {
                                            System.out.println("6.6");
                                            if (executor[1].equals(executorDetails1)) {
                                                assignment.put(executorDetails1, availableSlotsList.get(j));
                                                AssignedExecutors++;
                                                AssignedSlots++;
                                                count++;
                                            }
                                        }
                                    }
                                }
                                count = 0;
                                System.out.println("6.7");
                            }
                        }
                    }
                    System.out.println("6.8");
                    //count = 0;
                    //i--;
                }
                System.out.println("6.9");
            }
            System.out.println("7");
            if (AssignedExecutors < TotalExecutors) {

                for (int i = 0; i < executors.size(); i++) {
                    assignment.put(executors.get(i), availableSlots.get(i % availableSlots.size()));
                }

                System.out.println("8");
//                for (int i = 0; i < availableSlots.size(); i++) {
//                    if (!cluster.isSlotOccupied(availableSlots.get(i))) {
//                        assignment.put(executors.get(i), availableSlots.get(i));
//                        availableSlots.remove(i);
//                        AssignedExecutors++;
//                        AssignedSlots++;
//                    }
//                }
            }
            Map<WorkerSlot, List<ExecutorDetails>> nodePortToExecutors = Utils.reverseMap(assignment);
            for (Map.Entry<WorkerSlot, List<ExecutorDetails>> entry : nodePortToExecutors.entrySet()) {
                WorkerSlot nodePort = entry.getKey();
                List<ExecutorDetails> executor = entry.getValue();
                cluster.assign(nodePort, t, executor);
            }
            System.out.println("9");
        }
        return assignment;
    }

    /**
     * Function that read the hard coded values from YAML. file
     */
    private ArrayList ReadFromYaml() throws Exception {
        System.out.println("5.1");
        ArrayList check = new ArrayList();
        try {
            Scanner in = new Scanner(new File("/home/malik/Downloads/apache-storm-2.0.1-SNAPSHOT/conf/file.txt"));
            String s;
            while (in.hasNext()) {
                s = in.nextLine();
                if (s.equalsIgnoreCase("# Test traffic")) {
                    while (in.hasNext()) {
                        s = in.nextLine();
                        if (s.equalsIgnoreCase("# Resources")) {
                            break;
                        }
                        check.add(s.split(","));
                    }
                }
            }
            in.close();
            System.out.println("5.2");
            return check;
        } catch (FileNotFoundException ex) {
            throw new Exception("File specified not found");
        }
    }

    private void AllocateSlotsToNodes(Map<ExecutorDetails, WorkerSlot> slotMap) {

    }

    private ArrayList ReadResourceFromYaml() throws Exception {
        ArrayList check = new ArrayList();
        try {
            Scanner in = new Scanner(new File("/home/malik/Downloads/apache-storm-2.0.1-SNAPSHOT/conf/file.txt"));
            String s;
            while (in.hasNext()) {
                s = in.nextLine();
                if (s.equalsIgnoreCase("# Resources")) {
                    while (in.hasNext()) {
                        check.add(in.nextLine().split(","));
                    }
                }
            }
            System.out.println("6.1");
            in.close();
            return check;
        } catch (FileNotFoundException ex) {
            throw new Exception("File specified not found");
        }
    }

}
