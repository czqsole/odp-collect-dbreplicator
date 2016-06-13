package com.odp.collect.dbreplicator;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.omg.PortableInterceptor.ServerIdHelper;

import com.continuent.tungsten.common.config.PropertyException;
import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.Applier;
import com.continuent.tungsten.replicator.applier.ApplierWrapper;
import com.continuent.tungsten.replicator.applier.ParallelApplier;
import com.continuent.tungsten.replicator.applier.RawApplier;
import com.continuent.tungsten.replicator.conf.PropertiesManager;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntimeConf;
import com.continuent.tungsten.replicator.extractor.Extractor;
import com.continuent.tungsten.replicator.extractor.ExtractorWrapper;
import com.continuent.tungsten.replicator.extractor.ParallelExtractor;
import com.continuent.tungsten.replicator.extractor.RawExtractor;
import com.continuent.tungsten.replicator.filter.FilterManualProperties;
import com.continuent.tungsten.replicator.management.OpenReplicatorManager;
import com.continuent.tungsten.replicator.management.OpenReplicatorManagerMBean;
import com.continuent.tungsten.replicator.management.OpenReplicatorPlugin;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.plugin.ReplicatorPlugin;
import com.continuent.tungsten.replicator.thl.THLStoreApplier;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.odp.collect.dbreplicator.extractor.mysql.MySQLExtractor;
import com.odp.collect.dbreplicator.manager.ReplicationServiceManager;

public class Main {
	private Logger							logger = Logger.getLogger(Main.class);
	private String							CONFIG_FILE = "replicator.properties";
	
	private int 							serverId = 0;
	private BinaryLogClient 				binaryLogClient = null;
	private String 							url = "192.168.43.140";  /* "192.168.43.137" */                  
	private ReplicationServiceManager       replicationServiceManager;
	private OpenReplicatorManagerMBean 		orm = null;
	
	private TungstenProperties 				properties = null;
	
	public int getServerId() {
		return serverId;
	}

	public void setServerId(int serverId) {
		this.serverId = serverId;
	}
	
	public void configure() throws Exception {
		File confDir = ReplicatorRuntimeConf.locateReplicatorConfDir();
        File propsFile = new File(confDir, CONFIG_FILE);
        properties = PropertiesManager.loadProperties(propsFile);
        //properties.store(null);
        
        //System.out.println(properties.get("port"));
        
        /* RunTimeConf 暂时不用了*/
		/*replicationServiceManager = new ReplicationServiceManager();
		
		String serverName = "Test";
		orm = replicationServiceManager.createInternalService(serverName);
		
		String pluginClassName = "com.continuent.tungsten.replicator.management.tungsten.TungstenPlugin";
		OpenReplicatorPlugin plugin;
		try
        {
            plugin = (OpenReplicatorPlugin) Class.forName(pluginClassName)
                    .newInstance();
            //if (plugin instanceof FilterManualProperties)
            //    ((FilterManualProperties) plugin).setConfigPrefix(pluginPrefix);
            //else
            //    pluginProperties.applyProperties(plugin);
        }
        catch (PropertyException e)
        {
            throw new ReplicatorException(
                    "Unable to configure plugin properties: key="
                            + " class name=" + pluginClassName
                            + " : " + e.getMessage(), e);
        }
        catch (InstantiationException e)
        {
            throw new ReplicatorException("Unable to load plugin class: key="
                     + " class name=" + pluginClassName, e);
        }
        catch (IllegalAccessException e)
        {
            throw new ReplicatorException("Unable to load plugin class: key="
                     + " class name=" + pluginClassName, e);
        }
        catch (ClassNotFoundException e)
        {
            throw new ReplicatorException("Unable to load plugin class: key="
                     + " class name=" + pluginClassName, e);
        }

        // Plug-in is ready to go, so prepare it and call configure.
        try
        {
            plugin.prepare((OpenReplicatorManager)orm);
            plugin.configure(null);
        }
        catch (ReplicatorException e)
        {
            throw new ReplicatorException("Unable to configure plugin: key="
                     + " class name=" + pluginClassName, e);
        }
        catch (Throwable t)
        {
            String message = "Unable to configure plugin: key=" 
                    + " class name=" + pluginClassName;
            logger.error(message, t);
            throw new ReplicatorException(message, t);
        }*/
	}
	
	public void prepare() throws ReplicatorException, InterruptedException {
		Pipeline pipeline = new Pipeline();
		
		Stage stage = new Stage(pipeline);
		
		PluginContext context = null;
		
		SingleThreadStageTask singleThreadStageTask = new SingleThreadStageTask(stage, 0);
		singleThreadStageTask.setSchedule(new SimpleSchedule(stage, singleThreadStageTask));
		
		MySQLExtractor mextractor = new MySQLExtractor();
		mextractor.prepare(properties);
		mextractor.setUrl(url);
		mextractor.setBinlogSeq(17);
		String lastEvent = getLastEventByGtid("113547eb-022a-11e6-ae7e-000c299d4e24:55");
		//String lastEvent = getLastEventByGtid("113547eb-022a-11e6-ae7e-000c299d4e24:40");
		//mextractor.setLastEventId("mysql-bin.000176:455;-1;113547eb-022a-11e6-ae7e-000c299d4e24:37");
		System.out.println("lastEvent:" + lastEvent);
		mextractor.setServerId(serverId);
		mextractor.setLastEventId(lastEvent);
		//ReplicatorPlugin extractor = stage.getExtractorSpec()
        //        .instantiate(0);
        //if (extractor instanceof RawExtractor)
        //    extractor = new ExtractorWrapper((RawExtractor) extractor);
		//ReplicatorPlugin extractor = new ExtractorWrapper((RawExtractor) extractor);

        //if (extractor instanceof ParallelExtractor)
        //    ((ParallelExtractor) extractor).setTaskId(0);
		
		ReplicatorPlugin extractor = new ExtractorWrapper((RawExtractor) mextractor);
        
		//extractor.configure(context);
		//这里需要同步一下ExtractorWrapper中seqo，也就是event.seqo
        singleThreadStageTask.setExtractor((Extractor) extractor);
        
        //ReplicatorPlugin applier = stage.getApplierSpec()
        //		.instantiate(0);
        //if (applier instanceof RawApplier)
        //    applier = new ApplierWrapper((RawApplier) applier);

        //if (applier instanceof ParallelApplier)
        //    ((ParallelApplier) applier).setTaskId(0);
		
		//ReplicatorPlugin applier = new ApplierWrapper((RawApplier) applier);
        THLStoreApplier applier = new THLStoreApplier();

        //applier.configure(context);
        applier.prepare(context);
        singleThreadStageTask.setApplier((Applier) applier);
        
		Thread thread = new Thread(singleThreadStageTask,"task1");
		//thread.start();
	}
	
	public void start() {
		
	}
	
	public String getLastEventByGtid(String gtid) {
		binaryLogClient = new BinaryLogClient(url, 3306, "root", "root");
		binaryLogClient.setGtidSet("");//gtid
		binaryLogClient.setGtidToFind(gtid);
		StringBuilder sb = new StringBuilder();
		try {
			 binaryLogClient.connect();
			 System.out.println("文件名：" + binaryLogClient.getBinlogFilename());  
		     System.out.println("位置：" + binaryLogClient.getBinlogPosition());
		     System.out.println("serverId:" + binaryLogClient.getServerId());
		     
		     serverId = (int) binaryLogClient.getServerId();
		     
		     sb.append(binaryLogClient.getBinlogFilename());
		     sb.append(":");
		     sb.append(binaryLogClient.getGtidPos());
		     sb.append(";-1;");
		     sb.append(gtid);
		     
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sb.toString();
	}
	
	public static void main(String[] args) throws Exception {
		Main main = new Main();
		main.configure();
		main.prepare();
	}
}
