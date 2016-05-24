package com.odp.collect.dbreplicator;

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
import com.odp.collect.dbreplicator.extractor.Mysql.MySQLExtractor;

public class Main {
	public void prepare() throws ReplicatorException, InterruptedException {
		Pipeline pipeline = new Pipeline();
		
		Stage stage = new Stage(pipeline);
		
		PluginContext context = null;
		
		SingleThreadStageTask singleThreadStageTask = new SingleThreadStageTask(stage, 0);
		singleThreadStageTask.setSchedule(new SimpleSchedule(stage, singleThreadStageTask));
		
		MySQLExtractor mextractor = new MySQLExtractor();
		mextractor.prepare();
		mextractor.setLastEventId("mysql-bin.000169:795;-1");
		//ReplicatorPlugin extractor = stage.getExtractorSpec()
        //        .instantiate(0);
        //if (extractor instanceof RawExtractor)
        //    extractor = new ExtractorWrapper((RawExtractor) extractor);
		//ReplicatorPlugin extractor = new ExtractorWrapper((RawExtractor) extractor);

        //if (extractor instanceof ParallelExtractor)
        //    ((ParallelExtractor) extractor).setTaskId(0);
		
		ReplicatorPlugin extractor = new ExtractorWrapper((RawExtractor) mextractor);
        //extractor.configure(context);
        singleThreadStageTask.setExtractor((Extractor) extractor);
        
        //ReplicatorPlugin applier = stage.getApplierSpec()
        //		.instantiate(0);
        //if (applier instanceof RawApplier)
        //    applier = new ApplierWrapper((RawApplier) applier);

        //if (applier instanceof ParallelApplier)
        //    ((ParallelApplier) applier).setTaskId(0);
		
		//ReplicatorPlugin applier = new ApplierWrapper((RawApplier) applier);

        //applier.configure(context);
        //singleThreadStageTask.setApplier((Applier) applier);
        
		Thread thread = new Thread(singleThreadStageTask,"task1");
		thread.start();
	}
	
	public static void main(String[] args) throws ReplicatorException, InterruptedException {
		Main main = new Main();
		main.prepare();
	}
}
