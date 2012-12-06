package com.netease.webbench.blogbench.kv.mongodb;

import com.netease.webbench.blogbench.dao.BlogDAO;
import com.netease.webbench.blogbench.dao.BlogDaoFactory;
import com.netease.webbench.blogbench.misc.BbTestOptions;
import com.netease.webbench.common.DbOptions;

public class MongoDbBlogDaoFactory implements BlogDaoFactory {

	@Override
	public BlogDAO getBlogDao(DbOptions dbOpt, BbTestOptions bbTestOpt)
			throws Exception {
		// TODO Auto-generated method stub
		return new MongoDbBlogDao(dbOpt.getHost(), dbOpt.getPort());
	}

}