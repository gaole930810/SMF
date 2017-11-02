package com.Test;
import com.VMDServiceAPI.VMDClient;

public class CTest {
	private static String url="hdfs://vm1:9000/yty/video/Test4.rmvb";
	public static void main(String[] args){
		url=args[1];
		switch(args[0]){
		case "DELETE":
			VMDClient.DELETE(url).print();
			break;
		case "GETFRAME":
			VMDClient.GET_FRAME(url,args[2]).print();
			break;
		case "GENERATE":
			VMDClient.GENERATE(url).print();
		    break;
		case "UPLOAD"://上传视频
			VMDClient.UPLOAD(url).print();
			break;
		case "LS"://列出所有节点的所有VMD，及其I帧字典规模
			VMDClient.LS(url).print();
			break;
		case "GETVMD"://列出目标VMD的所有信息
			VMDClient.GET(url).print();
			break;
		    default:
		    	break;
		}
    }
}
