package com.odp.collect.dbreplicator.manager;

import java.io.File;
import java.io.FilenameFilter;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
//import com.continuent.tungsten.common.security.AuthenticationInfo;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.conf.PropertiesManager;
import com.continuent.tungsten.replicator.conf.ReplicatorConf;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntimeConf;
import com.continuent.tungsten.replicator.management.OpenReplicatorManager;
import com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean;
//import com.continuent.tungsten.replicator.management.ReplicationServiceManager;


public class ReplicationServiceManager implements ReplicationServiceManagerMBean
{
	public static final String CONFIG_SERVICES    = "services.properties";
    public static final String CONFIG_FILE_PREFIX = "static-";
    public static final String CONFIG_FILE_SUFFIX = ".properties";

    private static Logger                               logger                = Logger
            .getLogger(ReplicationServiceManager.class);
    private TungstenProperties                          serviceProps          = null;
    private TreeMap<String, OpenReplicatorManagerMBean> replicators           = new TreeMap<String, OpenReplicatorManagerMBean>();
    private Map<String, TungstenProperties>             serviceConfigurations = new TreeMap<String, TungstenProperties>();

    private int masterListenPortStart = 2111;
    private int masterListenPortMax   = masterListenPortStart;

    private String   managerRMIHost     = null;
    private int      managerRMIPort     = -1;
    private TimeZone hostTimeZone       = null;
    private TimeZone replicatorTimeZone = null;
    private OpenReplicatorManagerMBean orm = null;

    //private AuthenticationInfo securityInfo = null;

    /**
     * Creates a new <code>ReplicatorManager</code> object
     * 
     * @throws Exception
     */
    public ReplicationServiceManager() throws Exception
    {
    }
	
	public void start() throws ReplicatorException {
		// Find and load the service.properties file.
        File confDir = ReplicatorRuntimeConf.locateReplicatorConfDir();
        File propsFile = new File(confDir, CONFIG_SERVICES);
        serviceProps = PropertiesManager.loadProperties(propsFile);
        
        loadServiceConfigurations();
        
        for (String serviceName : serviceConfigurations.keySet())
        {
            TungstenProperties replProps = serviceConfigurations
                    .get(serviceName);
            String serviceType = replProps
                    .getString(ReplicatorConf.SERVICE_TYPE);
            boolean isDetached = replProps.getBoolean(ReplicatorConf.DETACHED);

            replProps.setBoolean(ReplicatorConf.FORCE_OFFLINE, false);

            if (serviceType.equals("local"))
            {
                // Get properties file name if specified or generate default.
                try
                {
                    logger.info(String.format(
                            "Starting the %s/%s replication service '%s'",
                            (isDetached ? "detached" : "internal"), serviceType,
                            serviceName));
                    startReplicationService(replProps);
                }
                catch (Exception e)
                {
                    logger.error(
                            "Unable to instantiate replication service: name="
                                    + serviceName,
                            e);
                }
            }
            else if (serviceType.equals("remote"))
            {
                //remoteServices.add(replProps);
            }
            else
            {
                logger.warn(String.format(
                        "The replication service '%s' has an urecognized type '%s'",
                        serviceName, serviceType));
            }
            
        }
	}
    
	private void loadServiceConfigurations() throws ReplicatorException
    {
        File confDir = ReplicatorRuntimeConf.locateReplicatorConfDir();

        FilenameFilter serviceConfigFilter = new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                return name.startsWith(CONFIG_FILE_PREFIX)
                        && name.endsWith(CONFIG_FILE_SUFFIX);
            }
        };

        serviceConfigurations.clear();

        // Make sure we have a list of files, sorted by name
        Map<String, File> serviceConfFiles = new TreeMap<String, File>();
        File[] fileArray = confDir.listFiles(serviceConfigFilter);
        for (File configFile : fileArray)
        {
            serviceConfFiles.put(configFile.getName(), configFile);
        }

        for (File serviceConf : serviceConfFiles.values())
        {
            // Name starts in the form static-<host>.<service>.properties
            String serviceConfName = serviceConf.getName();

            // get <host>.<service>.properties
            serviceConfName = serviceConfName
                    .substring(serviceConfName.indexOf(CONFIG_FILE_PREFIX)
                            + CONFIG_FILE_PREFIX.length());

            // get <host>.<service>
            String baseFileName = serviceConfName.substring(0,
                    serviceConfName.indexOf(CONFIG_FILE_SUFFIX));

            // This should just be the service name.
            String serviceName = baseFileName
                    .substring(baseFileName.lastIndexOf(".") + 1);

            TungstenProperties replProps = OpenReplicatorManager
                    .getConfigurationProperties(serviceName);

            serviceConfigurations.put(serviceName, replProps);
        }

    }
	
	private void startReplicationService(TungstenProperties replProps)
            throws ReplicatorException
    {
        String serviceName = replProps.getString(ReplicatorConf.SERVICE_NAME);
        String serviceType = replProps.getString(ReplicatorConf.SERVICE_TYPE);
        boolean isDetached = replProps.getBoolean(ReplicatorConf.DETACHED);

        //OpenReplicatorManagerMBean orm = null;

        try
        {
            if (isDetached)
            {
                throw new ReplicatorException(
                        "Creating of detached service is no longer supported");
            }
            else
            {
                orm = createInternalService(serviceName);
            }

            // Put the service in the list of replicators now, as the start
            // might fail.
            replicators.put(serviceName, orm);
            //orm.start(replProps.getBoolean(ReplicatorConf.FORCE_OFFLINE));

            int listenPort = orm.getMasterListenPort();
            if (listenPort > masterListenPortMax)
                masterListenPortMax = listenPort;

            logger.info(String.format(
                    "%s/%s replication service '%s' started successfully",
                    (isDetached ? "detached" : "internal"), serviceType,
                    serviceName));
        }
        catch (Exception e)
        {
            logger.error(
                    String.format("Unable to start replication service '%s'",
                            serviceName),
                    e);
        }

    }
	
	public OpenReplicatorManagerMBean createInternalService(String serviceName)
            throws ReplicatorException
    {
        logger.info("Starting replication service: name=" + serviceName);
        try
        {
            OpenReplicatorManager orm = new OpenReplicatorManager(serviceName);
            orm.setRmiHost(managerRMIHost);
            orm.setRmiPort(managerRMIPort);
            orm.setHostTimeZone(hostTimeZone);
            orm.setReplicatorTimeZone(replicatorTimeZone);
            orm.advertiseInternal();
            //orm.setSecurityInfo(this.securityInfo);
            return (OpenReplicatorManagerMBean) orm;
        }
        catch (Exception e)
        {
            throw new ReplicatorException(String.format(
                    "Unable to instantiate replication service '%s'",
                    serviceName), e);
        }
    }
	
	@Override
	public List<Map<String, String>> services() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isAlive() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Map<String, String> status() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean loadService(String name) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean unloadService(String name) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Map<String, String> resetService(String name) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> resetService(String name,
			Map<String, String> controlParams) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> replicatorStatus(String name) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> getStatus() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void stop() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void kill() throws Exception {
		// TODO Auto-generated method stub
		
	}

}
