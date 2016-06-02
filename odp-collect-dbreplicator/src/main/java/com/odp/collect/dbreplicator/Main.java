package com.odp.collect.dbreplicator;

import java.io.IOException;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.applier.Applier;
import com.continuent.tungsten.replicator.applier.ApplierWrapper;
import com.continuent.tungsten.replicator.applier.ParallelApplier;
import com.continuent.tungsten.replicator.applier.RawApplier;
import com.continuent.tungsten.replicator.conf.ReplicatorRuntime;
import com.continuent.tungsten.replicator.extractor.Extractor;
import com.continuent.tungsten.replicator.extractor.ExtractorWrapper;
import com.continuent.tungsten.replicator.extractor.ParallelExtractor;
import com.continuent.tungsten.replicator.extractor.RawExtractor;
import com.continuent.tungsten.replicator.plugin.PluginContext;
import com.continuent.tungsten.replicator.plugin.ReplicatorPlugin;
import com.continuent.tungsten.replicator.thl.THLStoreApplier;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.odp.collect.dbreplicator.extractor.mysql.MySQLExtractor;

public class Main {
	public void prepare() throws ReplicatorException, InterruptedException {
		Pipeline pipeline = new Pipeline();
		
		Stage stage = new Stage(pipeline);
		
		PluginContext context = null;
		
		SingleThreadStageTask singleThreadStageTask = new SingleThreadStageTask(stage, 0);
		singleThreadStageTask.setSchedule(new SimpleSchedule(stage, singleThreadStageTask));
		
		MySQLExtractor mextractor = new MySQLExtractor();
		mextractor.prepare();
		String lastEvent = getLastEventByGtid("113547eb-022a-11e6-ae7e-000c299d4e24:37");
		//mextractor.setLastEventId("mysql-bin.000176:455;-1;113547eb-022a-11e6-ae7e-000c299d4e24:37");
		System.out.println("lastEvent:" + lastEvent);
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
	
	public static String getLastEventByGtid(String gtid) {
		BinaryLogClient binaryLogClient = new BinaryLogClient("192.168.43.140", 3306, "root", "root");
		binaryLogClient.setGtidSet("");//gtid
		binaryLogClient.setGtidToFind(gtid);
		StringBuilder sb = new StringBuilder();
		try {
			 binaryLogClient.connect();
			 System.out.println("文件名：" + binaryLogClient.getBinlogFilename());  
		     System.out.println("位置：" + binaryLogClient.getBinlogPosition());
		     sb.append(binaryLogClient.getBinlogFilename());
		     sb.append(":");
		     sb.append(binaryLogClient.getBinlogPosition());
		     sb.append(";-1;");
		     sb.append(gtid);
		     
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sb.toString();
	}
	
	public static void main(String[] args) throws ReplicatorException, InterruptedException {
		Main main = new Main();
		main.prepare();
	}
}
