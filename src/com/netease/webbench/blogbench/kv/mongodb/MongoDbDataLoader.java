package com.netease.webbench.blogbench.kv.mongodb;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.netease.webbench.blogbench.dao.BlogDaoFactory;
import com.netease.webbench.blogbench.dao.SimpleDataLoader;
import com.netease.webbench.blogbench.misc.BbTestOptions;
import com.netease.webbench.blogbench.misc.ParameterGenerator;
import com.netease.webbench.common.DbOptions;

public class MongoDbDataLoader extends SimpleDataLoader {
	private Mongo mongo = null;
	private DB db = null;
	
	public MongoDbDataLoader(DbOptions dbOpt, BbTestOptions bbTestOpt,
			ParameterGenerator paraGen, BlogDaoFactory daoFactory) 
					throws UnknownHostException {
		super(dbOpt, bbTestOpt, paraGen, daoFactory);
		this.mongo = new Mongo(dbOpt.getHost(), dbOpt.getPort());
	}
	
	@Override
	public void pre() throws Exception {
		super.pre();
		
		// create collection and index
		this.db = mongo.getDB("test");
		this.db.createCollection("Blog", null);
		DBCollection coll = this.db.getCollection("Blog");
		
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put(MongoDbBlogDao.UID_FIELD, 1);
		searchQuery.put(MongoDbBlogDao.PTIME_FIELD, 1);
		searchQuery.put(MongoDbBlogDao.ALLOWVIEW_FIELD, 1);
		coll.ensureIndex(searchQuery);
	}
	
	@Override
	public void post() throws Exception{
		super.post();
		printStatistics();
	}
	
	/**
	 *  print statistics
	 */
	private void printStatistics() {
		System.out.println("Total time waste:  " 
				+ statis.getTotalTimeWaste() + "  milliseconds");
		System.out.println("Load data waste: " + statis.getLoadDataTimeWaste() 
				+ "  milliseconds");
	}

	@Override
	public String getLoadSummary() {
		StringBuilder buf = new StringBuilder(512);
		buf.append("Test collection name: Blog\n")
			.append("Test collection size: " + bbTestOpt.getTbSize() + "\n")
			.append("Total time waste: " + statis.getTotalTimeWaste() + " milliseconds\n")
			.append("Create collection waste: " + statis.getCreateTableTimeWaste() + "  milliseconds\n")
			.append("Load data waste: " + statis.getLoadDataTimeWaste() + "  milliseconds\n");
		return buf.toString();
	}	
}
