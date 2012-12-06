package com.netease.webbench.blogbench.kv.mongodb;

import com.netease.webbench.blogbench.BlogbenchPlugin;
import com.netease.webbench.blogbench.dao.BlogDaoFactory;
import com.netease.webbench.blogbench.dao.DataLoader;
import com.netease.webbench.blogbench.misc.BbTestOptions;
import com.netease.webbench.blogbench.misc.ParameterGenerator;
import com.netease.webbench.common.DbOptions;

public class BlogbenchMongoDbPlugin implements BlogbenchPlugin {
	private BlogDaoFactory daoFactory = new MongoDbBlogDaoFactory();
			
	@Override
	public void validateOptions(DbOptions dbOpt, BbTestOptions bbTestOpt)
			throws IllegalArgumentException {
		if (!"mongodb".equalsIgnoreCase(dbOpt.getDbType()))
			throw new IllegalArgumentException("Wrong database type: " + dbOpt.getDbType() +
					", should be 'mongodb' while you use mongodb plugin!");
	}

	@Override
	public DataLoader getDataLoader(DbOptions dbOpt, BbTestOptions bbTestOpt,
			ParameterGenerator parGen) throws Exception {
		return new MongoDbDataLoader(dbOpt, bbTestOpt, parGen, daoFactory);
	}

	@Override
	public BlogDaoFactory getBlogDaoFacory() throws Exception {
		return daoFactory;
	}

}
