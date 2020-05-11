// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
var operateJSON;
var colCounter;
var colInterval = 50;
var rowInterval = 15;
var nodeservers = [];
var cb = [];
var rb = [];
var rcb = [];
var scb = [];
var bcb = [];
var dc = [];
var dcDD = [];
var dcChoices = [];
var stype = [];
var serverChoices = [];
var serverDD = [];
var storage = [];
var storageChoices = [];
var storageDD = [];
var table, header, row, radioGroup;
var refreshButton, addToListButton, removeFromListButton, addToClusterButton, removeFromClusterButton, restartServicesButton;
var refreshButtonOn, addToListButtonOn, removeFromListButtonOn, addToClusterButtonOn, removeFromClusterButtonOn, restartServicesButtonOn;
var restartNodesButton, restartNodesButtonOn;

function setOpsView() {
	setRectangleView("Operations");
 	
	if (pageLoad) {
		setDataCenters();
		setServerTypes();
		setStorageChoices();
		setButtons();
	}
 	
 	if (getSelectedNodeCount() > 0)
 		setStatusTableView();
 	else
 		setOpsTableView();
 	
 	setButtonState();
 	
 	if (pageLoad)
		pageLoad = false;
}

function setButtons() {
	//var textvoff = 13;

	svgContainer.append("foreignObject").attr("width", htmlopwidth).attr("height", htmlopheight)
	.attr("x","" + (327)).attr("y","" + (90) + "")
	.html("<input id=\"addToListButtonID\" type=\"command\" class=\"opButtonEnabled\" value=\"Add To List\" onclick=\"window['addToListButtonOn']();\" />");
	addToListButtonOn = function() {addToList();};
	//document.getElementById('addToListButtonID').onselectstart= function() {return false;};
	
	svgContainer.append("foreignObject").attr("width", htmlopwidth).attr("height", htmlopheight)
	.attr("x","" + (452)).attr("y","" + (90) + "")
	.html("<input id=\"removeFromListButtonID\" type=\"command\" class=\"opButtonEnabled\" value=\"Remove From List\" onclick=\"window['removeFromListButtonOn']();\" />");
	
	removeFromListButtonOn = function() {removeFromList();};
	
	svgContainer.append("foreignObject").attr("width", htmlopwidth).attr("height", htmlopheight)
	.attr("x","" + (327)).attr("y","" + (115) + "")
	.html("<input id=\"addToClusterButtonID\" type=\"command\" class=\"opButtonEnabled\" value=\"Add To Cluster\" onclick=\"window['addToClusterButtonOn']();\" />");
	addToClusterButtonOn = function() {
		if (validateServersForAdd()) {
			var webJson = new Object();
			var operateJSON = getPostBackParametersFromServers();
			operateJSON.action = "OP_ADD";
			webJson.operate = operateJSON;
			//alert(JSON.stringify(webJson));
			doAjax("/ajaxHandler", JSON.stringify(webJson), clusterOperationNotification, reportError);
		}
		else {
			alert("Only one data node and/or its replicas can be specified per add request");
		}
	};
	
	svgContainer.append("foreignObject").attr("width", htmlopwidth).attr("height", htmlopheight)
	.attr("x","" + (452)).attr("y","" + (115) + "")
	.html("<input id=\"removeFromClusterButtonID\" type=\"command\" class=\"opButtonEnabled\" value=\"Remove From Cluster\" onclick=\"window['removeFromClusterButtonOn']();\" />");
	removeFromClusterButtonOn = function() {
		var webJson = new Object();
		var operateJSON = getPostBackParametersFromSelection();
		operateJSON.action = "OP_DELETE";
		webJson.operate = operateJSON;
		//alert(JSON.stringify(webJson));
		doAjax("/ajaxHandler", JSON.stringify(webJson), clusterOperationNotification, reportError);
	};
	
	svgContainer.append("foreignObject").attr("width", htmlopwidth).attr("height", htmlopheight)
	.attr("x","" + (327)).attr("y","" + (140) + "")
	.html("<input id=\"restartNodesButtonID\" type=\"command\" class=\"opButtonEnabled\" value=\"Restart Nodes\" onclick=\"window['restartNodesButtonOn']();\" />");
	restartNodesButtonOn = function() {
		var webJson = new Object();
		var operateJSON = getPostBackParametersFromSelection();
		operateJSON.action = "OP_RESTART";
		webJson.operate = operateJSON;
		//alert(JSON.stringify(webJson));
		doAjax("/ajaxHandler", JSON.stringify(webJson), clusterOperationNotification, reportError);
	};
	
	svgContainer.append("foreignObject").attr("width", htmlopwidth).attr("height", htmlopheight)
	.attr("x","" + (452)).attr("y","" + (140) + "")
	.html("<input id=\"restartServicesButtonID\" type=\"command\" class=\"opButtonEnabled\" value=\"Restart Services\" onclick=\"window['restartServicesButtonOn']();\" />");
	restartServicesButtonOn = function() {
		var webJson = new Object();
		var operateJSON = getPostBackParametersFromSelection();
		operateJSON.action = "OP_REFRESH";
		webJson.operate = operateJSON;
		//alert(JSON.stringify(webJson));
		doAjax("/ajaxHandler", JSON.stringify(webJson), clusterOperationNotification, reportError);
	};
}

function clusterOperationNotification(result, textStatus, jQxhr) {
	var ajaxJson = JSON.parse(result);
	nodeservers = [];
	notificationPanel.text("Cluster request queued for " + ajaxJson.operate.clusterStatus.length + " servers for action " + ajaxJson.action); 
	messagePanel.text("OK");
	setClusterStatusASync();
}

function setStatusTableView() {
	if (typeof table != 'undefined')
		table.remove();
	
	table = svgContainer.append("foreignObject").attr("width", 700).attr("height", 350).attr("y",180).attr("x",(100))
	.html("<table width=\"700px\" height = \"350px\" id=\"statusTableID\"></table>");
	
	//table = svgContainer.append("g");
   // table.attr("id","OpsGroup").attr("transform", "translate(50,120)");
	
    setStatusTableHeader();

    var rowIndex = 0;
    for (var j=0; j< clusterStatus.length; j++)
		if (nodes && nodes[j]) {
			nodes[j].on("click",createOperationsNodeSelectionFunction(j));
			if (nodes[j].attr("selected") == "true") {
				setStatusTableRow(j,rowIndex++);
			}
		}
}

function setOpsTableView() {
	var check = [];
	if (typeof cb != 'undefined') {
    	for (var j=0; j< cb.length; j++) {
    		if (typeof cb[j] != 'undefined' && cb[j].getAttribute('checked') == 'true')
    			check[j] = true;
    		else
    			check[j] = false;
    		//alert(checkstring[j]);
    	}
	}
	
	if (typeof table != 'undefined')
		table.remove();
	
	table = svgContainer.append("foreignObject").attr("width", 700).attr("height", 350).attr("y",180).attr("x",(100))
	.html("<table width=\"700px\" height = \"350px\" id=\"opTableID\"></table>");
    
	setOpsTableHeader();
    for (var j=0; j< clusterStatus.length; j++)
		if (nodes && nodes[j])
			nodes[j].on("click",createOperationsNodeSelectionFunction(j));
    
 	for (var i=0; i< nodeservers.length; i++) {
 		setOpsTableRow(i, check[i]);
 	}
}

function setStatusTableRow(index, rowIndex) {
	var htmlTable = document.getElementById('statusTableID');
	htmlTable.className = 'opstatustable';
	var row = htmlTable.insertRow(rowIndex);
    row.id = 'statusTableRowID' + rowIndex;
    htmlTable.appendChild(row);
    if (rowIndex % 2 == 0)
    	row.className = 'opstatustablerow';
    else
    	row.className = 'opstatustablerowalt';
    var th1 = document.createElement('td'); 
    th1.innerHTML = clusterStatus[index].logicalNumber;
    th1.className = 'opstatustablecell';
    row.appendChild(th1);
    var th2 = document.createElement('td'); 
    var link = document.createElement("a");
    if (typeof clusterStatus[index].address != 'undefined' && clusterStatus[index].address.split(".").length == 4)
    	link.setAttribute("href", "http://" + clusterStatus[index].address + ":8080/operate");
    link.innerHTML = clusterStatus[index].address;
    th2.appendChild(link);
    th2.className = 'opstatustablecell';
    row.appendChild(th2);
    var th3 = document.createElement('td'); 
    th3.className = 'opstatustablecell';
    th3.innerHTML = clusterStatus[index].dataCenter;
    row.appendChild(th3);
    var th4 = document.createElement('td');
    th4.className = 'opstatustablecell';
    th4.innerHTML = clusterStatus[index].instanceId;
    row.appendChild(th4);
    var th5 = document.createElement('td');
    th5.className = 'opstatustablecell';
    th5.innerHTML = clusterStatus[index].name;
    row.appendChild(th5);
    var th6 = document.createElement('td'); 
    th6.className = 'opstatustablecell';
    th6.innerHTML = clusterStatus[index].isSeedNode;
    row.appendChild(th6);
    var th7 = document.createElement('td'); 
    th7.className = 'opstatustablecell';
    th7.innerHTML = clusterStatus[index].isBootstrapNode;
    row.appendChild(th7);
    var th8 = document.createElement('td'); 
    th8.className = 'opstatustablecell';
    th8.innerHTML = clusterStatus[index].replicaOf;
    row.appendChild(th8);
    var th9 = document.createElement('td'); 
    th9.className = 'opstatustablecell';
    th9.innerHTML = clusterStatus[index].stype;
    row.appendChild(th9);
    var th10 = document.createElement('td'); 
    th10.className = 'opstatustablecell';
    th10.innerHTML = clusterStatus[index].storage;
    row.appendChild(th10);
    var th11 = document.createElement('td'); 
    th11.className = 'opstatustablecell';
    th11.innerHTML = clusterStatus[index].isNodeUp?"Up":"Down";
    row.appendChild(th11);
    var th12 = document.createElement('td'); 
    th12.className = 'opstatustablecell';
    th12.innerHTML = clusterStatus[index].isOverlayUp?"Up":"Down";
    row.appendChild(th12);
}

function setOpsTableRow(index, selected) {
	
	var htmlTable = document.getElementById('opTableID');
	htmlTable.className = 'optable';
	var row = htmlTable.insertRow(index);
    row.id = 'opTableRowID' + index;
    htmlTable.appendChild(row);
    if (index % 2 == 0)
    	row.className = 'optablerow';
    else
    	row.className = 'optablerowalt';
    
    var th1 = document.createElement('td'); 
    cb[index] = createButtonCheckBox('cbID' + index, 'rowenabled', 'rowdisabled');
    cb[index].setAttribute("ln",'' + nodeservers[index].ln);
    if (selected) {
    	cb[index].setAttribute('checked','true');
    	cb[index].setAttribute('class', 'rowenabled');
    }
    th1.appendChild(cb[index]);
    th1.className = 'optablecell';
    
    row.appendChild(th1);
    
    var th2 = document.createElement('td'); 
    th2.className = 'optablecell';
    th2.innerHTML = nodeservers[index].ln;
    
    row.appendChild(th2);
    
    var th3 = document.createElement('td'); 
    th3.className = 'optablecell';
    row.appendChild(th3);
    var dataCentersDD = document.createElement("select");
    dataCentersDD.id = 'dataCentersDD' + index;
    dataCentersDD.onchange = createServerSelectFunction(dataCentersDD, index, "dc");
    th3.appendChild(dataCentersDD);
    dataCentersDD.className = 'opselectinrow';
    populateSelect('dataCentersDD' + index,dcChoices);
    dataCentersDD.value = nodeservers[index].dc;

    var th4 = document.createElement('td'); 
    th4.className = 'optablecell';
    row.appendChild(th4);
    var serverChoicesDD = document.createElement("select");
    serverChoicesDD.id = 'serverChoicesDD' + index;
    serverChoicesDD.onchange = createServerSelectFunction(serverChoicesDD, index, "stype");
    th4.appendChild(serverChoicesDD);
    serverChoicesDD.className = 'opselectinrow';
    populateSelect('serverChoicesDD' + index,serverChoices);
    serverChoicesDD.value = nodeservers[index].stype;
    
    var th5 = document.createElement('td'); 
    th5.className = 'optablecell';
    row.appendChild(th5);
    var storageChoicesDD = document.createElement("select");
    storageChoicesDD.id = 'storageChoicesDD' + index;
    storageChoicesDD.onchange = createServerSelectFunction(storageChoicesDD, index, "storage");
    th5.appendChild(storageChoicesDD);
    storageChoicesDD.className = 'opselectinrow';
    populateSelect('storageChoicesDD' + index,storageChoices);
    storageChoicesDD.value = nodeservers[index].storage;
    
    var th6 = document.createElement('td'); 
    rcb[index] = createButtonCheckBox('rcbID' + index, 'rowenabled', 'rowdisabled');
    th6.appendChild(rcb[index]);
    th6.className = 'optablecell';
    rcb[index].onclick = createServerCheckboxFunction(rcb[index], index, "replica", 'rowenabled', 'rowdisabled'); //overrides default function
    row.appendChild(th6);
    if (nodeservers[index].replica) {
    	rcb[index].setAttribute('checked','true');
    	rcb[index].setAttribute('class', 'rowenabled');
    }
    else {
    	rcb[index].setAttribute('checked','false');
    	rcb[index].setAttribute('class', 'rowdisabled');
    }
    
    var th7 = document.createElement('td'); 
    scb[index] = createButtonCheckBox('scbID' + index, 'rowenabled', 'rowdisabled');
    th7.appendChild(scb[index]);
    th7.className = 'optablecell';
    scb[index].onclick = createServerCheckboxFunction(scb[index], index, "seed", 'rowenabled', 'rowdisabled'); //overrides default function
    row.appendChild(th7);
    if (nodeservers[index].seed) {
    	scb[index].setAttribute('checked','true');
    	scb[index].setAttribute('class', 'rowenabled');
    }
    else {
    	scb[index].setAttribute('checked','false');
    	scb[index].setAttribute('class', 'rowdisabled');
    }
    
    var th8 = document.createElement('td'); 
    bcb[index] = createButtonCheckBox('bcbID' + index, 'rowenabled', 'rowdisabled');
    th8.appendChild(bcb[index]);
    th8.className = 'optablecell';
    bcb[index].onclick = createServerCheckboxFunction(bcb[index], index, "bootstrap", 'rowenabled', 'rowdisabled'); //overrides default function 
    row.appendChild(th8);
    if (nodeservers[index].bootstrap) {
    	bcb[index].setAttribute('checked','true');
    	bcb[index].setAttribute('class', 'rowenabled');
    }
    else {
    	bcb[index].setAttribute('checked','false');
    	bcb[index].setAttribute('class', 'rowdisabled');
    }
}

function setStatusTableHeader() {
	
    var htmlTable = document.getElementById('statusTableID');
    var htmlHeader = document.createElement("thead");
    htmlHeader.className = 'opstatustableheader';
    htmlHeader.id = 'statusTableHeaderID';
    htmlHeader.rowSpace = "2";
    htmlTable.appendChild(htmlHeader);
    var th1 = document.createElement('th'); 
    th1.innerHTML = "Ln";
    th1.className = 'opstatustablecell';
    //th1.width = '100px';
    htmlHeader.appendChild(th1);
    var th2 = document.createElement('th'); 
    th2.className = 'opstatustablecell';
    th2.innerHTML = "Address";
    //th2.width = '100px';
    htmlHeader.appendChild(th2);
    var th3 = document.createElement('th'); 
    th3.innerHTML = "Data<br>Center";
    //th3.width = '100px';
    htmlHeader.appendChild(th3);
    var th4 = document.createElement('th'); 
    th4.innerHTML = "Instance<br>Id";
    //th4.width = '100px';
    htmlHeader.appendChild(th4);
    var th5 = document.createElement('th'); 
    th5.innerHTML = "Name";
    htmlHeader.appendChild(th5);
    var th6 = document.createElement('th'); 
    th6.innerHTML = "Seed<br>Node?";
    htmlHeader.appendChild(th6);
    var th7 = document.createElement('th'); 
    th7.innerHTML = "Bootstrap<br>Node?";
    htmlHeader.appendChild(th7);
    var th8 = document.createElement('th'); 
    th8.innerHTML = "Replica<br>Of";
    htmlHeader.appendChild(th8);
    var th9 = document.createElement('th'); 
    th9.innerHTML = "Type";
    htmlHeader.appendChild(th9);
    var th10 = document.createElement('th'); 
    th10.innerHTML = "Size";
    htmlHeader.appendChild(th10);
    var th11 = document.createElement('th'); 
    th11.innerHTML = "Storage<br>Status";
    htmlHeader.appendChild(th11);
    var th12 = document.createElement('th'); 
    th12.innerHTML = "View<br>Status";
    htmlHeader.appendChild(th12);
   
    addCssClass('statusTableID','opstatustable');
    addCssClass('statusTableHeaderID','opstatustableheader');
}

function setOpsTableHeader() {
	var htmlTable = document.getElementById('opTableID');
    var htmlHeader = document.createElement("thead");
    htmlHeader.className = 'optableheader';
    htmlHeader.id = 'opTableHeaderID';
    htmlTable.appendChild(htmlHeader);
    
    var th1 = document.createElement('th'); 
    th1.innerHTML = "";
    th1.className = 'optablecell';
    htmlHeader.appendChild(th1);
    th1.width = '30px';
    var th2 = document.createElement('th'); 
    th2.innerHTML = "Ln";
    th2.className = 'optablecell';
    htmlHeader.appendChild(th2);
    th2.width = '30px';
    var th3 = document.createElement('th'); 
    th3.innerHTML = "Data Center";
    th3.className = 'optablecell';
    htmlHeader.appendChild(th3);
    th3.width = '80px';
    var th4 = document.createElement('th'); 
    th4.innerHTML = "Type";
    th4.className = 'optablecell';
    htmlHeader.appendChild(th4);
    th4.width = '80px';
    var th5 = document.createElement('th'); 
    th5.innerHTML = "Storage Size (GB)";
    th5.className = 'optablecell';
    htmlHeader.appendChild(th5);
    th5.width = '80px';
    var th6 = document.createElement('th'); 
    th6.innerHTML = "Is Replica?";
    th6.className = 'optablecell';
    htmlHeader.appendChild(th6);
    th6.width = '30px';
    var th7 = document.createElement('th'); 
    th7.innerHTML = "Is Seed?";
    th7.className = 'optablecell';
    htmlHeader.appendChild(th7);
    th7.width = '30px';
    var th8 = document.createElement('th'); 
    th8.innerHTML = "Is Bootstrap?";
    th8.className = 'optablecell';
    htmlHeader.appendChild(th8);
    th8.width = '40px';
    
    addCssClass('opTableID','optable');
    addCssClass('opTableHeaderID','optableheader');

}

function createServerCheckboxFunction(checkBox, index, field, checkedStyle, uncheckedStyle) {
	var enabled = checkedStyle;
	var disabled = uncheckedStyle;
	var box = checkBox;
	var i = index;
	var fld = field;
    return function () {
    	//box.setAttribute('checked',(box.getAttribute('checked') == 'true')?'false':'true');
  		if (box.getAttribute('checked') == 'true') {
  			updateServerValue(i, fld, false);
  			box.setAttribute('checked','false');
  			box.setAttribute('class', disabled);
  		}
  		else {
  			updateServerValue(i, fld, true);
  			box.setAttribute('checked','true');
  			box.setAttribute('class', enabled);
  		}
	};
}

function createServerSelectFunction(selectObject, index, field) {
	var sel = selectObject;
	var i = index;
	var fld = field;
    return function () {
  		updateServerValue(i, fld, sel.value);
    };
}

function updateServerValue(i, field, value) {
	//TODO refactor this method via dynamic javascript 
	//eval("servers[" + i + "]." + field + " = " + value);
	//alert(i);
	//alert(field);
	//alert(value);
	switch(field) {
		case "ln":
			nodeservers[i].ln = value;
			break;
		case "dc":
			nodeservers[i].dc = value;
			break;
		case "stype":
			nodeservers[i].stype = value;
			break;
		case "storage":
			nodeservers[i].storage = value;
			break;
		case "replica":
			nodeservers[i].replica = value;
			break;
		case "seed":
			nodeservers[i].seed = value;
			break;
		case "cluster":
			nodeservers[i].cluster = value;
			break;
		case "bootstrap":
			nodeservers[i].bootstrap = value;
			break;  					
	}
}

function resetServers() {
	var max = -1;
	for (var i=0; i< clusterStatus.length; i++) {
		if (clusterStatus[i].logicalNumber > max)
			max = clusterStatus[i].logicalNumber;
	}
	//alert(servers.length);
	for (var j=0; j< nodeservers.length; j++) {
		nodeservers[j].ln = ++max;	
		//alert(max);
	}
}

function setDataCenters() {
	dcChoices[0] = "ap-northeast-1";
	dcChoices[1] = "ap-southeast-1";
	dcChoices[2] = "ap-southeast-2";
	dcChoices[3] = "eu-central-1";
	dcChoices[4] = "eu-west-1";
	dcChoices[5] = "sa-east-1";
	dcChoices[6] = "us-east-1";
	dcChoices[7] = "us-west-1";
	dcChoices[8] = "us-west-2";
}

function setServerTypes() {
	serverChoices = [];
	//serverChoices[serverChoices.length] = "Micro";
	serverChoices[serverChoices.length] = "Small";
	//serverChoices[serverChoices.length] = "Medium";
	serverChoices[serverChoices.length] = "Large";
}

function setStorageChoices() {
	storageChoices[0] = 32;
	storageChoices[1] = 800;
	//storageChoices[2] = 1000;
}

function validateServersForAdd() {
	if (getDataNode() < 0)
		return false;
	else
		return true;
}

function getDataNode() {
	var dataNode = -1;
	for (var i=0; i< nodeservers.length; i++) {
		if (!nodeservers[i].replica) {
			if (dataNode > 0)
				return -1;
			dataNode = nodeservers[i].ln;
		}
	}
	return dataNode;
}

function getPostBackParametersFromSelection() {
	var clusterCopy = new Object();
	clusterCopy.cluster = currentCluster;
	clusterCopy.clusterStatus = [];
	for (var j=0; j<clusterStatus.length; j++) {
		if (nodes && nodes[j] && nodes[j].attr("selected") == "true")
			clusterCopy.clusterStatus[clusterCopy.clusterStatus.length] = clusterStatus[j];
	}
	return clusterCopy;
}

function getPostBackParametersFromServers() {
	var clusterCopy = new Object();
	clusterCopy.cluster = currentCluster;
	clusterCopy.clusterStatus = [];
	for (var i=0; i < nodeservers.length; i++) {
		var node = new Object();
		node.logicalNumber = nodeservers[i].ln;
		node.dataCenter = nodeservers[i].dc;
		node.stype = nodeservers[i].stype;
		node.storage = nodeservers[i].storage;
		node.cluster = nodeservers[i].cluster;
		if (nodeservers[i].replica)
			node.replicaOf = getDataNode();
		else
			node.replicaOf = nodeservers[i].ln;
		node.isBootstrapNode = nodeservers[i].bootstrap;
		node.isSeedNode = nodeservers[i].seed;
		clusterCopy.clusterStatus[clusterCopy.clusterStatus.length] = node;
	}
	return clusterCopy;
}

function addToList() {
	//alert("adding to list");
	var nextServer = nodeservers.length;
	nodeservers[nextServer] = new Object();
 	nodeservers[nextServer].ln = getNextLn();
 	nodeservers[nextServer].dc = dcChoices[0];
 	nodeservers[nextServer].stype = serverChoices[0];
 	nodeservers[nextServer].storage = storageChoices[0];
 	nodeservers[nextServer].replica = false;
 	nodeservers[nextServer].seed = false;
 	nodeservers[nextServer].bootstrap = false;
 	nodeservers[nextServer].cluster = currentCluster;
 	setOpsView();
}

function removeFromList() {
	var list = getCheckedList();
	for (var i=0; i< list.length; i++) {
		for (var j=0; j<nodeservers.length; j++) {
			if (nodeservers[j].ln == list[i]) {
				removeCheck(nodeservers[j].ln);
				nodeservers.splice(j,1);
				break;
			}
		}
	}
	resetServers();
	setOpsView();
}

function getCheckedList() {
	var list = [];
	for (var i=0; i< cb.length; i++) {
		
		if (cb[i].getAttribute('checked') == 'true') {
			list[list.length] = cb[i].getAttribute("ln");
		}
	}
	return list;
}

function removeCheck(ln) {
	for (var i=0; i< cb.length; i++) {
		if (cb[i].getAttribute("ln") == ln)
			cb[i].setAttribute("checked","false");
	}
}

function getNextLn() {
	//alert("getting next ln");
	var max = -1;
	for (var i=0; i< nodeservers.length; i++) {
		if (nodeservers[i].ln > max)
			max = nodeservers[i].ln;
	}
	if (max >= 0)
		return ++max;
	else {
		for (var i=0; i<clusterStatus.length; i++) {
			if (clusterStatus[i].logicalNumber > max)
				max = clusterStatus[i].logicalNumber;
		}
		//alert(max);
		return ++max;
	}
}

function createDropDown(index, field, source, container, dropDown, choices, x, y) {
	var xloc = x + 100;
	var yloc = y - 30;
	var src = source;
	var dd = dropDown;
	var ch = choices;
	var con = container;
	var fld = field;
	var idx = index;
	return function() {
		var vinterval = 15;
		for (var i=0; i < ch.length; i++) {
			if(dd[i]) {
				dd[i].remove();
			}
			var rect = document.getElementById(fld + "rect" + i);
			if (rect)
				rect.parentNode.removeChild(rect);
			
			con.append("rect").attr("x",xloc).attr("y",yloc+i*vinterval)
			.attr("width","100").attr("height","15").attr("fill","white")
			.attr("fill-opacity","1").attr("id",fld + "rect" + i);
			
			dd[i] = con.append("text").text(ch[i]).attr("x",xloc).attr("y",yloc+i*vinterval+10)
			.attr("font-size","7px").attr("fill","blue").attr("visibility","visible")
			.attr("fill-opacity","1").attr("id",fld + i);

			dd[i].attr("x",xloc).attr("y",yloc + i*vinterval).attr("visibility","visible");
			dd[i].on("click",createDropDownSelection(fld,src,dd,idx,i));
			//document.getElementById("datacenter" + i).style.zIndex="-1";	
		}
	};
}

function createDropDownSelection(field, source, dropDown, index, valIndex) {
	var src = source;
	var i = index;
	var vi = valIndex;
	var dd = dropDown;
	var fld = field;
	return function() {
		if (!dd || !src)
			return;
		//alert(i);
		//alert(vi);
		//alert(fld);
		//alert(dd[vi].text());
		
		src.text(dd[vi].text());
		updateServerValue(i, fld, dd[vi].text());
		for (var j=0; j< dd.length; j++) {
			dd[j].attr("visibility","collapse");
			var rect = document.getElementById(fld + "rect" + j);
			if (rect)
				rect.parentNode.removeChild(rect);
		}
	};
}

function createOperationsNodeSelectionFunction(idx) {
	var i = idx;
    return function () {
		nodes[i].attr("selected",nodes[i].attr("selected")=="false"?"true":"false");
  		if (nodes[i].attr("selected")=="true") {
  			nodes[i].attr("rx", "" + selectedEllipse);
  			nodes[i].attr("ry", "" + selectedEllipse);
  			nodes[i].attr("stroke-opacity","1");
  		}
  		else {
  			nodes[i].attr("rx", "" + unselectedEllipse);
  			nodes[i].attr("ry", "" + unselectedEllipse);
  			nodes[i].attr("stroke-opacity","0");
  		}
  		while (nodeservers.length) {nodeservers.pop(); }
  		setOpsView();
	};
}

function setButtonState() {
/*
		if (!isBoot) {		// disable cluster operations controls from everywhere other than the bootstrap node 
			
			document.getElementById('addToListButtonID').disabled = true;
			document.getElementById('addToListButtonID').setAttribute('class','opButtonDisabled');
			
			document.getElementById('removeFromListButtonID').disabled = true;
			document.getElementById('removeFromListButtonID').setAttribute('class','opButtonDisabled');
			
			document.getElementById('addToClusterButtonID').disabled = true;
			document.getElementById('addToClusterButtonID').setAttribute('class','opButtonDisabled');

			document.getElementById('removeFromClusterButtonID').disabled = true;
			document.getElementById('removeFromClusterButtonID').setAttribute('class','opButtonDisabled');
			
			document.getElementById('restartNodesButtonID').disabled = true;
			document.getElementById('restartNodesButtonID').setAttribute('class','opButtonDisabled');
			
			document.getElementById('restartServicesButtonID').disabled = true;
			document.getElementById('restartServicesButtonID').setAttribute('class','opButtonDisabled');
			
			return;
		}
		
*/
	
		if (getSelectedNodeCount() > 0) {
			document.getElementById('addToListButtonID').disabled = true;
			document.getElementById('addToListButtonID').setAttribute('class','opButtonDisabled');
			
			document.getElementById('removeFromListButtonID').disabled = true;
			document.getElementById('removeFromListButtonID').setAttribute('class','opButtonDisabled');
			
			document.getElementById('addToClusterButtonID').disabled = true;
			document.getElementById('addToClusterButtonID').setAttribute('class','opButtonDisabled');

			document.getElementById('removeFromClusterButtonID').disabled = false;
			document.getElementById('removeFromClusterButtonID').setAttribute('class','opButtonEnabled');
			
			document.getElementById('restartNodesButtonID').disabled = false;
			document.getElementById('restartNodesButtonID').setAttribute('class','opButtonEnabled');
			
			document.getElementById('restartServicesButtonID').disabled = false;
			document.getElementById('restartServicesButtonID').setAttribute('class','opButtonEnabled');

  		}
  		else if (nodeservers.length > 0){
  			document.getElementById('addToListButtonID').disabled = false;
			document.getElementById('addToListButtonID').setAttribute('class','opButtonEnabled');
			
			document.getElementById('removeFromListButtonID').disabled = false;
			document.getElementById('removeFromListButtonID').setAttribute('class','opButtonEnabled');
			
			document.getElementById('addToClusterButtonID').disabled = false;
			document.getElementById('addToClusterButtonID').setAttribute('class','opButtonEnabled');

			document.getElementById('removeFromClusterButtonID').disabled = true;
			document.getElementById('removeFromClusterButtonID').setAttribute('class','opButtonDisabled');
			
			document.getElementById('restartNodesButtonID').disabled = true;
			document.getElementById('restartNodesButtonID').setAttribute('class','opButtonDisabled');
			
			document.getElementById('restartServicesButtonID').disabled = true;
			document.getElementById('restartServicesButtonID').setAttribute('class','opButtonDisabled');

  		}
  		else {
  			document.getElementById('addToListButtonID').disabled = false;
			document.getElementById('addToListButtonID').setAttribute('class','opButtonEnabled');
			
			document.getElementById('removeFromListButtonID').disabled = true;
			document.getElementById('removeFromListButtonID').setAttribute('class','opButtonDisabled');
			
			document.getElementById('addToClusterButtonID').disabled = true;
			document.getElementById('addToClusterButtonID').setAttribute('class','opButtonDisabled');

			document.getElementById('removeFromClusterButtonID').disabled = true;
			document.getElementById('removeFromClusterButtonID').setAttribute('class','opButtonDisabled');
			
			document.getElementById('restartNodesButtonID').disabled = true;
			document.getElementById('restartNodesButtonID').setAttribute('class','opButtonDisabled');
			
			document.getElementById('restartServicesButtonID').disabled = true;
			document.getElementById('restartServicesButtonID').setAttribute('class','opButtonDisabled');

  		}
}