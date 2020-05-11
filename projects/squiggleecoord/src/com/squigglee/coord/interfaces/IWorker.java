package com.squigglee.coord.interfaces;

public interface IWorker {
	public interface IMaster {
		public void register();
		public void initialize();
		public void processTasks();
	}
}
