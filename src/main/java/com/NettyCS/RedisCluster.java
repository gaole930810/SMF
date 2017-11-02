package com.NettyCS;

import java.util.HashSet;
import java.util.Set;

public class RedisCluster {
	public String ClusterName;
	public Set<String> RedisClusterIPs = new HashSet<String>();
	public String ClusterPassport;
	public RedisCluster(String ClusterName,Set<String> RedisClusterIPs,String ClusterPassport){
		this.ClusterName=ClusterName;
		this.RedisClusterIPs=RedisClusterIPs;
		this.ClusterPassport=ClusterPassport;
	}

}
