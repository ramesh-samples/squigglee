package com.squigglee.coord.interfaces;

import java.util.List;
import java.util.Map;

import com.squigglee.core.entity.CommandType;

public interface IEntitlementService  extends ICoordService {
	public Map<String, Map<Integer, List<CommandType>>> getEntitlements(String cluster, int ln);
	public void setEntitlement(String clusterSubj, int lnSubj, String clusterObj, int lnObj, List<CommandType> permissions);
	public void deleteEntitlement(String clusterSubj, int lnSubj, String clusterObj, int lnObj);
}
