package com.odp.collect.dbreplicator.manager;

import java.util.List;
import java.util.Map;

//import com.continuent.tungsten.common.jmx.DynamicMBeanHelper;

public interface ReplicationServiceManagerMBean {
	/**
     * Lists currently defined replicators and whether they are running or not.
     */
    public List<Map<String, String>> services() throws Exception;

    /**
     * Returns true if the MBean is alive. Used to test liveness of connections.
     */
    public boolean isAlive();

    /**
     * Returns status information.
     * 
     * @throws Exception
     */
    public Map<String, String> status() throws Exception;

    /**
     * Starts a replication service.
     * 
     * @param name Name of the replicator service
     * @return True if replicator service exists and was started
     * @throws Exception Thrown if service start-up fails
     */
    public boolean loadService(String name) throws Exception;

    /**
     * Stops a replication service.
     * 
     * @param name Name of the replicator service
     * @return True if replicator service exists and was stopped
     * @throws Exception Thrown if service stop fails
     */
    public boolean unloadService(String name) throws Exception;

    /**
     * Resets a replication service.
     * 
     * @param name Name of the replicator service
     * @return Map of strings that indicate actions taken.
     * @throws Exception Thrown if service stop fails
     */
    public Map<String, String> resetService(String name) throws Exception;

    /**
     * Resets a replication service or some of its components (thl, relay,
     * database).
     * 
     * @param name Name of the replicator service
     * @param controlParams 0 or more control parameters expressed as name-value
     *            pairs (option={-all|-thl|-relay|-db})
     * @return Map of strings that indicate actions taken.
     * @throws Exception Thrown if service stop fails
     */
    public Map<String, String> resetService(String name,
            Map<String, String> controlParams) throws Exception;

    /**
     * Returns a list of properties that have the status for each of the current
     * services.
     */
    public Map<String, String> replicatorStatus(String name) throws Exception;

    /**
     * Returns a map of status properties for all current replicators
     * 
     * @throws Exception
     */
    public Map<String, String> getStatus() throws Exception;

    /**
     * Stops all replication services and exits the process cleanly.
     * 
     * @throws Exception Thrown if service stop fails
     */
    public void stop() throws Exception;

    /**
     * Terminates the replicator process immediately without clean-up. This
     * command should be used only if stop does not work or for testing.
     */
    public void kill() throws Exception;

    /**
     * Returns a helper that supplies MBean metadata.
     */
    //public abstract DynamicMBeanHelper createHelper() throws Exception;
}
