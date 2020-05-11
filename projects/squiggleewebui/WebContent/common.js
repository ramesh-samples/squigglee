// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
var pageLoad;
var activeLn, isBoot, currentCluster, inError;
var pageUpdateFunction;	
var commonJSON ;
var dataTypes = [], sketchTypes = [], frequencyTypes = [], windowSizes = [], dimensionSizes = [];
var clusterStatus = [];  
var statusTimer;
var availableNodeParameters = []; 
var parameterCounter;
var counter = 0;
var st;
var messagePanel, notificationPanel;
var unselectedEllipse = 10; selectedEllipse = 15;
var cx = 310; var cy = 310; var rx = 200; var ry = 200; 
var x=50; var y=50; var w = 800; var h = 500;
var r = Math.sqrt(rx*rx + ry*ry);
//var rb = [];
var nodes = [];
var nodeLabels = [];
var pages = [];
var dcs = [];
var centeringDiv, svgContainer, clusterRing, serverCoords, title, title2, color;
var titlex = 15, titley = 23;
var buttonx = 170, buttony = 120;
var xstart = 65, ystart = 60;
var linktableobj, linktable, linkwidth = 50, linkheight = 15, linkxstart = 340, linkystart = 7; //old linystart = 580
var bwidth = 50, bheight = 15,	opwidth = 120, opheight = 20, htmlopwidth = 150, htmlopheight = 50;
var titleBar1, titleBar2;
var currentLink, operateLink, configureLink, matchLink, summarizeLink;

function getCoordinatesOnCircle(pointCount, cx, cy, rx, ry) {
	//alert("hi");
	var coords = [];
	var x; var y;
	var r = Math.sqrt(rx*rx + ry*ry);
	var interval = 2.0 * Math.PI / pointCount;
	var angle;
	var counter = 0;
	for (var i = 0; i < pointCount; i++) {
		angle = Math.PI / 2.0 - i * interval;
		x = cx + r * Math.cos(angle);
		y = cy - r * Math.sin(angle);
		coords[counter] = [];
		coords[counter][0] = x;
		coords[counter][1] = y;
		counter++;
	}	
	//alert(coords);
	return coords;
}

//count of points and rectangle dimensions [x,y] and width, height
function getCoordinatesOnRectangle(pointCount) {
	//alert("hi");
	var coords = [];
	var perimeter = 2*(w + h);
	var step = perimeter / pointCount;
	//alert(pointCount);
	//alert(step);
	for (var i = 0; i < pointCount; i++) {
		coords[i] = [];
		if (i*step <= (w / 2)) {								//leg 1
			coords[i][0] = x + w/2 + i*step;
			coords[i][1] = y;
		} else if (i*step <= (w / 2 + h)) {						//leg 2
			coords[i][0] = x + w;
			coords[i][1] = y + i*step - w/2;
		} else if (i*step <= (w / 2 + h + w)) {					//leg 3
			coords[i][0] = x + w - (i*step - w/2 - h);
			coords[i][1] = y + h;
		} else if (i*step <= (w / 2 + h + w + h)) {				//leg 4
			coords[i][0] = x;
			coords[i][1] = y + h - (i*step - w/2 - h - w);
		} else {												//leg 5
			coords[i][0] = x + (i*step - w/2 - h - w - h);
			coords[i][1] = y;
		}
	}
	//alert(coords);
	return coords;
}

function setRectangleView(titleString) {
	if (pageLoad)
		centeringDiv = d3.select("body").append("xhtml:div");
	if (pageLoad) {
		svgContainer = centeringDiv.append("svg").attr("width", 900).attr("height", 650);
		titleBar1 = svgContainer.append("rect").attr("x", 0).attr("y", 0).attr("width", (1000)).attr("height", (8)).attr("class","titlebar1");
		titleBar2 = svgContainer.append("rect").attr("x", 0).attr("y", 8).attr("width", (1000)).attr("height", (23)).attr("class","titlebar2");
		clusterRing = svgContainer.append("rect").attr("x", (x-3)).attr("y", (y-3)).attr("width", (w+6)).attr("height", (h+6)).attr("class","ring");
		clusterRing = svgContainer.append("rect").attr("x", x).attr("y", y).attr("width", w).attr("height", h).attr("class","ring");
		setTitle(titleString);
		setMessagePanel();
		setNotificationPanel();
		setLinks();
	}
	setNodes();
	dcs = getDataCenters(clusterStatus);
}


function setClusterStatusSync() {		//on initial page load only
	var webJson = new Object();
	var commonJson = new Object();
	commonJson.action = "ST_UPDATE";
	commonJson.clusterStatus = clusterStatus;
	webJson.common = commonJson;
	var returnJsonString = doSyncAjax("/ajaxHandler", JSON.stringify(webJson), reportError);
	var returnJson = JSON.parse(returnJsonString);
	clusterStatus = returnJson.common.clusterStatus;
	if (typeof clusterStatus != 'undefined' && clusterStatus.length > 0)
		currentCluster = clusterStatus[0].cluster;
	if (returnJson.common.isBoot == "true")
		isBoot = true;
	else
		isBoot = false;
	inError = false;
}

function updateClusterStatus(result, textStatus, jQxhr) {
	var returnJson = JSON.parse(result);
	clusterStatus = returnJson.common.clusterStatus;
	if (returnJson.common.isBoot == "true")
		isBoot = true;
	else
		isBoot = false;
	inError = false;
	pageUpdateFunction();
	if (typeof clusterStatus == 'undefined' || clusterStatus.length == 0) {
		currentCluster = null;
		notificationPanel.text("Empty cluster status found at " + (new Date()));
	}
	else {
		currentCluster = clusterStatus[0].cluster;
		notificationPanel.text("Updated status for cluster: " + clusterStatus[0].cluster + " at " + (new Date()));
	}
	messagePanel.text("");
}

function setClusterStatusASync() {		// on periodic status refreshes via javascript timer 
	var webJson = new Object();
	var commonJson = new Object();
	commonJson.action = "ST_UPDATE";
	commonJson.clusterStatus = clusterStatus;
	commonJson.isBoot = "false";
	webJson.common = commonJson;
	doAjax("/ajaxHandler", JSON.stringify(webJson), updateClusterStatus, reportError);
}

function setLinks() {
	
	if (typeof linktableobj != 'undefined')
		linktableobj.remove();
	
	linktableobj = svgContainer.append("foreignObject").attr("width", 400).attr("height", 150).attr("y",linkystart).attr("x",(linkxstart))
	.html("<table id=\"linktableID\" ></table>");
	var linktable = document.getElementById('linktableID');
	linktable.setAttribute('class','linktable');
	
	var homeButtonOn = function() {window.location = "/home";};
	var homeButton = document.createElement("button");
	homeButton.innerText = "Home";
	homeButton.id = "linkOperateButtonID";
	homeButton.readonly = true;
	if (currentLink != "home") {
		homeButton.setAttribute('class','linkButtonNewEnabled');
		homeButton.onclick = homeButtonOn;
	}
	else
		homeButton.setAttribute('class','linkButtonNewDisabled');
	
	var operateButtonOn = function() {window.location = "/operate";};
	var operateButton = document.createElement("button");
	operateButton.innerText = "Operation";
	operateButton.id = "linkOperateButtonID";
	operateButton.readonly = true;
	if (currentLink != "operate") {
		operateButton.setAttribute('class','linkButtonNewEnabled');
		operateButton.onclick = operateButtonOn;
	}
	else
		operateButton.setAttribute('class','linkButtonNewDisabled');
	
	var configureButtonOn = function() {window.location = "/configure";};
	var configureButton = document.createElement("button");
	configureButton.innerText = "Configuration";
	configureButton.id = "linkConfigureButtonID";
	configureButton.readonly = true;
	if (currentLink != "configure") {
		configureButton.setAttribute('class','linkButtonNewEnabled');
		configureButton.onclick = configureButtonOn;
	}
	else
		configureButton.setAttribute('class','linkButtonNewDisabled');
	
	var retrieveButtonOn = function() {window.location = "/retrieve";};
	var retrieveButton = document.createElement("button");
	retrieveButton.innerText = "Retrieval";
	retrieveButton.id = "linkRetrieveButtonID";
	retrieveButton.readonly = true;
	if (currentLink != "retrieve") {
		retrieveButton.setAttribute('class','linkButtonNewEnabled');
		retrieveButton.onclick = retrieveButtonOn;
	}
	else
		retrieveButton.setAttribute('class','linkButtonNewDisabled');
	
	var summarizeButtonOn = function() {window.location = "/summarize";};
	var summarizeButton = document.createElement("button");
	summarizeButton.innerText = "Synopses";
	summarizeButton.id = "linkSummarizeButtonID";
	summarizeButton.readonly = true;
	if (currentLink != "summarize") {
		summarizeButton.setAttribute('class','linkButtonNewEnabled');
		summarizeButton.onclick = summarizeButtonOn;
	}
	else
		summarizeButton.setAttribute('class','linkButtonNewDisabled');
	
	var linkrow = linktable.insertRow(0);

	var homeTd = document.createElement('td'); 
	homeTd.colSpan = "1";
	homeTd.appendChild(homeButton);
    linkrow.appendChild(homeTd);
    linkrow.appendChild(getBlankColumn(2));

	var operateTd = document.createElement('td'); 
	operateTd.colSpan = "1";
	operateTd.appendChild(operateButton);
    linkrow.appendChild(operateTd);
    linkrow.appendChild(getBlankColumn(2));
    
    var configureTd = document.createElement('td'); 
    configureTd.colSpan = "1";
    configureTd.appendChild(configureButton);
    linkrow.appendChild(configureTd);
    linkrow.appendChild(getBlankColumn(2));

    var retrieveTd = document.createElement('td'); 
    retrieveTd.colSpan = "1";
    retrieveTd.appendChild(retrieveButton);
    linkrow.appendChild(retrieveTd);
    linkrow.appendChild(getBlankColumn(2));
    
    var summarizeTd = document.createElement('td'); 
    summarizeTd.colSpan = "1";
    summarizeTd.appendChild(summarizeButton);
    linkrow.appendChild(summarizeTd);
}

function setNotificationPanel() {
	notificationPanel = svgContainer.append("text").attr("class",'notificationnormal').attr("x",52).attr("y",575).text("");
}

function setMessagePanel() {
	messagePanel = svgContainer.append("text").attr("class",'messagenormal').attr("x",840).attr("y",45);
}

function reportError(jqXhr, textStatus, errorThrown) {
	if (typeof messagePanel != 'undefined')
		messagePanel.text("FAIL");//.attr("class",'messagefailure');
	if (typeof notificationPanel != 'undefined')
		notificationPanel.text('ERROR: ' + errorThrown);//.attr("class",'notificationfailure');
	//alert('ERROR: ' + errorThrown);
	inError = true;
	setNodes();
}

function setNodes() {
	if (clusterStatus.length == 0) {
		if ((typeof nodes != 'undefined') && (typeof nodes[i] != 'undefined')) {
			nodes[i].remove();
			nodeLabels[i].remove();			// remove the old nodes
		}
		return;
	}
	
	serverCoords =  getCoordinatesOnRectangle(clusterStatus.length);
	
	var runNum = 0;
	var currentReplicaOf = clusterStatus[0].replicaOf;

	var selected = [];
	var nodeClickFunctions = [];
	if (typeof nodes != 'undefined') {
		for (var i=0; i< clusterStatus.length; i++) {
			for (var n = 0; n < nodes.length; n++) {
				if (nodes[n].attr("ln") == clusterStatus[i].logicalNumber) {
					if (typeof nodes[n].on("click") != 'undefined')
						nodeClickFunctions[i] = nodes[n].on("click");
					if (nodes[n].attr("selected") == "true") {
						selected[i] = true;	// preserve their prior ui selection status
						break;
					}
					else
						selected[i] = false;
				}
			}
		}
	}

	for (var i=0; i< clusterStatus.length; i++) {
		if (clusterStatus[i].replicaOf != currentReplicaOf) {
			runNum++;
			currentReplicaOf = clusterStatus[i].replicaOf;
		}
		var colClass = 'down';
		if (clusterStatus[i].isOverlayUp) {
			colClass = 'partial';
			if (clusterStatus[i].isNodeUp)
				colClass = 'ok';
		}
		if (inError)
			colClass = "error";
		if ((typeof nodes != 'undefined') && (typeof nodes[i] != 'undefined')) {
			nodes[i].remove();
			nodeLabels[i].remove();			// remove the old nodes
		}
		var lineClass = 'set';
		if (clusterStatus[i].logicalNumber == 0)
			lineClass = 'boot';
		else if (runNum % 2 == 1)
			lineClass = 'altset';
			nodes[i] = svgContainer.append("ellipse");
			nodes[i].attr("cx", serverCoords[i][0]).attr("cy", (serverCoords[i][1]))
				.attr("rx", selected[i]?("" + selectedEllipse):("" + unselectedEllipse))
				.attr("ry", selected[i]?("" + selectedEllipse):("" + unselectedEllipse));
		
		nodes[i].attr('class',colClass + ' ' + lineClass)
        	.attr("selected",selected[i]?"true":"false").attr("ln",clusterStatus[i].logicalNumber);
		if (typeof nodeClickFunctions[i] != 'undefined')
			nodes[i].on("click", nodeClickFunctions[i]);
		else
			nodes[i].on("click",createNodeSelectionFunction(clusterStatus[i].logicalNumber));
		//alert(runNum);
		nodeLabels[i] = svgContainer.append("text");
		nodeLabels[i].text(clusterStatus[i].logicalNumber).attr("x", (i<10)?(serverCoords[i][0]-2):((serverCoords[i][0]-4)))
			.attr("y", (serverCoords[i][1])+3).attr('class','nodelabel');
 	}
	
	for (var i= (clusterStatus.length); i < nodes.length; i++)		//discard the excess old nodes
		if (typeof nodes[i] != 'undefined') {
			nodes[i].remove();
			nodeLabels[i].remove();
		}
	
}

function setTitle(titleString) {
	title = svgContainer.append("text").text("Squigglee " + titleString)
		.attr("x","" + (titlex)).attr("y","" + titley)
		.attr('class','pagetitle');
}

function getDataCenters(clusterStatus) {
	var centers = [];
	for (var i=0; i< clusterStatus.length; i++) {
		var dcName = clusterStatus[i].dataCenter;
		if (centers.indexOf(dcName) == -1)
			centers[centers.length] = dcName;
	}
	centers.unshift("All");
	return centers;
}

function getSelectedNodeCount() {
	var count = 0;
	if (!nodes)
		return 0;
	for (var i=0; i < nodes.length; i++)
		if (nodes[i] && nodes[i].attr("selected") == "true")
			count++;
	return count;
}

function createNodeSelectionFunction(idx) {
	var i = idx;
    return function () {
    	//alert("Hi from base page node selection function");
		nodes[i].attr("selected",nodes[i].attr("selected")=="false"?"true":"false");
  		if (nodes[i].attr("selected")=="true") {
  			nodes[i].attr("rx", "" + selectedEllipse);
  			nodes[i].attr("ry", "" + selectedEllipse);
  		}
  		else {
  			nodes[i].attr("rx", "" + unselectedEllipse);
  			nodes[i].attr("ry", "" + unselectedEllipse);
  		}
	};
}

function createNewForm(formId) {
	var form = document.createElement("form");
	form.setAttribute('id',formId);
	form.setAttribute('method',"POST");
	//form.setAttribute('action',"...url");
	document.getElementsByTagName('body')[0].appendChild(form);
	return form;
}

function doAjax(url, jsondata, callbackFunction, errorFunction) {
	//alert("posting json data -- " + jsondata);
	jQuery.ajax({url : url, type: 'POST', data: jsondata, success: callbackFunction, error: errorFunction});
}

function doSyncAjax(url, jsondata, errorFunction) {
	//alert("posting json data -- " + jsondata);
	return jQuery.ajax({url : url, type: 'POST', data: jsondata, async: false, error: errorFunction}).responseText;
}

function getCheckedListFromCheckBoxGroup(checkBoxes) {
	if (!checkBoxes)
		return [];
	var list = [];
	for (var i=0; i< checkBoxes.length; i++) {
		if (checkBoxes[i].attr("checked")=="true") {
			list[list.length] = checkBoxes[i].attr("ln");
		}
	}
	return list;
}

function setDataTypes() {
	dataTypes[0] = "double";
	dataTypes[1] = "boolean";
	dataTypes[2] = "float";
	dataTypes[3] = "int";
	dataTypes[4] = "long";
	dataTypes[5] = "string";
	dataTypes[6] = "text";
	dataTypes[7] = "blob";
}

function setSketchTypes() {
	sketchTypes[0] = "CM";
	sketchTypes[1] = "EX";
}

function setFrequencyTypes() {
	frequencyTypes[0] = "MILLIS";
	frequencyTypes[1] = "SECONDS";
	frequencyTypes[2] = "MINUTES";
	frequencyTypes[3] = "HOURS";
	frequencyTypes[4] = "DAYS";
	frequencyTypes[5] = "YEARS";
	frequencyTypes[6] = "MICROS";
	frequencyTypes[7] = "NANOS";
}

function setWindowSizes() {
	windowSizes[0] = 50;
	windowSizes[1] = 100;
	windowSizes[2] = 200;
	windowSizes[3] = 500;
	windowSizes[4] = 1000;
}

function setDimensionSizes() {
	dimensionSizes[0] = 8;
	dimensionSizes[1] = 16;
	dimensionSizes[2] = 24;
	dimensionSizes[3] = 32;
	dimensionSizes[4] = 40;
	dimensionSizes[5] = 48;
	dimensionSizes[6] = 56;
	dimensionSizes[7] = 64;
}

function populateSelect(target, options) {
    var select = document.getElementById(target);
    if (select == null || typeof select == 'undefined')
    	return;
    if (select.options != null)
    	select.options.length = 0;
    for (var i = 0; i < options.length; i++) {
        var opt = document.createElement('option');
        opt.value = options[i];
        opt.innerHTML = options[i];
        select.appendChild(opt);
        opt.className = 'opselectinrow';
    }
}

function createCheckboxFunction(checkBox) {
    return function () {
		checkBox.attr("checked",checkBox.attr("checked")=="false"?"true":"false")
		.attr('class',(checkBox.attr("checked")=="true")?'checked':'unchecked');
	};
}

function getConfigParameterNames(nodeConfig) {
	var parms = [];
	if (typeof nodeConfig != 'undefined') {
		for (var i=0; i< nodeConfig.length; i++) {
			parms[i] = nodeConfig[i].parameter;
		}
	}
	return parms;
}

function getConfigValue(parameterName, nodeConfig, fieldName) {
	if (typeof nodeConfig == 'undefined' || typeof parameterName == 'undefined' || typeof fieldName == 'undefined')
		return null;
	for (var i=0; i< nodeConfig.length; i++) {
		if (nodeConfig[i].parameter == parameterName)
			return nodeConfig[i][fieldName];
	}
	return null;
}


function addCssClass(elementId, cssClass) {
	//todo replace with html5
	$('#' + elementId).addClass(cssClass);
}

function containsCssClass(elementId, cssClass) {
	//todo replace with html5
	$('#' + elementId).hasClass(cssClass);
}

function removeCssClass(elementId, cssClass) {
	//todo replace with html5
	$('#' + elementId).removeClass(cssClass);
}

function toggleCssClass(elementId, cssClass) {
	//todo replace with html5
	$('#' + elementId).toggleClass(cssClass);
}

//YEARS, DAYS, HOURS, MINUTES, SECONDS, MILLIS, MICROS, NANOS
function steps(freq, timestamp1, timestamp2) {
	var dt1 = new Date();
	var dt2 = new Date();
	switch (freq) {
	case 'NANOS':
		return (timestamp2.getTime() - timestamp1.getTime());
	case 'MICROS':
		return (timestamp2.getTime() - timestamp1.getTime());
	case 'DAYS':
		dt1 = new Date(timestamp1.getUTCFullYear(), timestamp1.getUTCMonth(), 1, 0, 0, 0, 0);
		dt2 = new Date(timestamp2.getUTCFullYear(), timestamp2.getUTCMonth(), 1, 0, 0, 0, 0);
		return Math.ceil(dt2.getTime() - dt1.getTime())/(24*60*60*1000);
	case 'HOURS':
		dt1 = new Date(timestamp1.getUTCFullYear(), timestamp1.getUTCMonth(), timestamp1.getUTCDate(), 0, 0, 0, 0);
		dt2 = new Date(timestamp2.getUTCFullYear(), timestamp2.getUTCMonth(), timestamp2.getUTCDate(), 0, 0, 0, 0);
		return Math.ceil(dt2.getTime() - dt1.getTime())/(60*60*1000);
	case 'MILLIS':
		return (timestamp2.getTime() - timestamp1.getTime());
	case 'MINUTES':
		dt1 = new Date(timestamp1.getUTCFullYear(), timestamp1.getUTCMonth(), timestamp1.getUTCDate(), timestamp1.getUTCHours(), 0, 0, 0);
		dt2 = new Date(timestamp2.getUTCFullYear(), timestamp2.getUTCMonth(), timestamp2.getUTCDate(), timestamp2.getUTCHours(), 0, 0, 0);
		return Math.ceil(dt2.getTime() - dt1.getTime())/(60*1000);
	case 'SECONDS':
		dt1 = new Date(timestamp1.getUTCFullYear(), timestamp1.getUTCMonth(), timestamp1.getUTCDate(), timestamp1.getUTCHours(), timestamp1.getUTCMinutes(), 0, 0);
		dt2 = new Date(timestamp2.getUTCFullYear(), timestamp2.getUTCMonth(), timestamp2.getUTCDate(), timestamp2.getUTCHours(), timestamp2.getUTCMinutes(), 0, 0);
		return Math.ceil(dt2.getTime() - dt1.getTime())/(1000);
	case 'YEARS':
		return (timestamp1.getUTCFullYear() - timestamp2.getUTCFullYear() + 1);
	default:
		break;
	}
	return null;
}


//YEARS, DAYS, HOURS, MINUTES, SECONDS, MILLIS, MICROS, NANOS
function advance(freq, timestamp, steps) {
	var dt = new Date();
	switch (freq) {
	case 'NANOS':
		dt.setTime(timestamp.getTime() + steps);
		break;
	case 'MICROS':
		dt.setTime(timestamp.getTime() + steps);
		break;
	case 'DAYS':
		dt = new Date(timestamp.getUTCFullYear(), timestamp.getUTCMonth(), 1, 0, 0, 0, 0);
		dt.setUTCDate(dt.getUTCDate() + steps);
		break;
	case 'HOURS':
		dt = new Date(timestamp.getUTCFullYear(), timestamp.getUTCMonth(), timestamp.getUTCDate(), 0, 0, 0, 0);
		dt.setUTCHours(dt.getUTCHours() + steps);
		break;
	case 'MILLIS':
		dt.setTime(timestamp.getTime() + steps);
		break;
	case 'MINUTES':
		dt = new Date(timestamp.getUTCFullYear(), timestamp.getUTCMonth(), timestamp.getUTCDate(), timestamp.getUTCHours(), 0, 0, 0);
		dt.setUTCMinutes(dt.getUTCMinutes() + steps);
		break;
	case 'SECONDS':
		dt = new Date(timestamp.getUTCFullYear(), timestamp.getUTCMonth(), timestamp.getUTCDate(), timestamp.getUTCHours(), timestamp.getUTCMinutes(), 0, 0);
		dt.setUTCSeconds(dt.getUTCSeconds() + steps);
		break;
	case 'YEARS':
		//var dt = new Date(year, month, day, hours, minutes, seconds, milliseconds);
		dt = new Date(timestamp.getUTCFullYear(), 0, 1, 0, 0, 0, 0);
		dt.setUTCFullYear(dt.getUTCFullYear() + steps);
		break;
	default:
		break;
	}
	return dt;
}

//YEARS, DAYS, HOURS, MINUTES, SECONDS, MILLIS, MICROS, NANOS
function retrace(freq, timestamp, steps) {
	var dt = new Date();
	switch (freq) {
	case 'NANOS':
		dt.setTime(timestamp.getTime());
		break;
	case 'MICROS':
		dt.setTime(timestamp.getTime());
		break;
	case 'DAYS':
		dt = new Date(timestamp.getUTCFullYear(), timestamp.getUTCMonth(), 1, 0, 0, 0, 0);
		dt.setUTCDate(dt.getUTCDate() - steps);
		break;
	case 'HOURS':
		dt = new Date(timestamp.getUTCFullYear(), timestamp.getUTCMonth(), timestamp.getUTCDate(), 0, 0, 0, 0);
		dt.setUTCHours(dt.getUTCHours() - steps);
		break;
	case 'MILLIS':
		dt.setTime(timestamp.getTime() - steps);
		break;
	case 'MINUTES':
		dt = new Date(timestamp.getUTCFullYear(), timestamp.getUTCMonth(), timestamp.getUTCDate(), timestamp.getUTCHours(), 0, 0, 0);
		dt.setUTCMinutes(dt.getUTCMinutes() - steps);
		break;
	case 'SECONDS':
		dt = new Date(timestamp.getUTCFullYear(), timestamp.getUTCMonth(), timestamp.getUTCDate(), timestamp.getUTCHours(), timestamp.getUTCMinutes(), 0, 0);
		dt.setUTCSeconds(dt.getUTCSeconds() - steps);
		break;
	case 'YEARS':
		//var dt = new Date(year, month, day, hours, minutes, seconds, milliseconds);
		dt = new Date(timestamp.getUTCFullYear(), 0, 1, 0, 0, 0, 0);
		dt.setUTCFullYear(dt.getUTCFullYear() - steps);
		break;
	default:
		break;
	}
	return dt;
}

//YEARS, DAYS, HOURS, MINUTES, SECONDS, MILLIS, MICROS, NANOS
function getTickFormat(freq, d, offset) {
	switch(freq) {
		case 'MILLIS': return function(d,i) {
				return d.getUTCMilliseconds();
		};
		case 'SECONDS': return function(d,i) {
				return d.getUTCSeconds();
		};
		case 'MINUTES': return function(d,i) {
				return d.getUTCMinutes();
		};
		case 'HOURS': return function(d,i) {
				return d.getUTCHours();
		};
		case 'MICROS': return function(d,i) {
				return +offset;
			};
		case 'NANOS': return function(d,i) {
				return +offset;
			};
		case 'DAYS': return function(d,i) {
				return d.getUTCFullYear() + "/" + (d.getUTCMonth()+1) + "/" + d.getUTCDate();
		};
		case 'YEARS': return function(d,i) { 
				return (d.getUTCFullYear()); 
		};
	}
} 

function getRow1TickFormat(freq, d) {
	switch(freq) {
		case 'MILLIS': 
				return (d.getUTCFullYear() + "/" + (d.getUTCMonth()+1) + "/" + d.getUTCDate()); 
		case 'SECONDS': 
			return (d.getUTCFullYear() + "/" + (d.getUTCMonth()+1) + "/" + d.getUTCDate());
		case 'MINUTES': 
			return (d.getUTCFullYear() + "/" + (d.getUTCMonth()+1) + "/" + d.getUTCDate());
		case 'HOURS': 
			return (d.getUTCFullYear() + "/" + (d.getUTCMonth()+1) + "/" + d.getUTCDate());
		case 'MICROS': 
			return (d.getUTCFullYear() + "/" + (d.getUTCMonth()+1) + "/" + d.getUTCDate());
		case 'NANOS': 
			return (d.getUTCFullYear() + "/" + (d.getUTCMonth()+1) + "/" + d.getUTCDate());
		case 'DAYS': 
			return ("");
		case 'YEARS': 
				return (""); 
	}
}

function getRow2TickFormat(freq, d) {
	switch(freq) {
		case 'MILLIS': 
			return (d.getUTCHours() + ":" + d.getUTCMinutes() + ":" + d.getUTCSeconds() + " sec"); 
		case 'SECONDS': 
			return (d.getUTCHours() + ":" + d.getUTCMinutes() + " min"); 
		case 'MINUTES': 
			return ("" + d.getUTCHours() + " hrs");
		case 'HOURS': 
			return "";
		case 'MICROS': 
			return (d.getUTCHours() + ":" + d.getUTCMinutes() + ":" + d.getUTCSeconds() + ":" + d.getUTCMilliseconds() + " mil"); 
		case 'NANOS': 
			return (d.getUTCHours() + ":" + d.getUTCMinutes() + ":" + d.getUTCSeconds() + ":" + d.getUTCMilliseconds() + "mil"); 
		case 'DAYS': 
			return ""; 
		case 'YEARS': 
			return ""; 
	}
}


function getBlankSpan() {
	var blankSpan =  document.createElement("span");
	blankSpan.innerHTML = "&nbsp;";
	return blankSpan;
}

function getBlankColumn(colspan) {
	var blankSpan =  getBlankSpan();
	var blankCell = document.createElement('td'); 
	blankCell.colSpan = "" + parseInt(colspan);
	blankCell.appendChild(blankSpan);
    return blankCell;
}

function getHRColumn(colspan) {
	var blankSpan =  document.createElement("span");
	blankSpan.innerHTML = "<hr>";
	var blankCell = document.createElement('td'); 
	blankCell.colSpan = "" + parseInt(colspan);
	blankCell.appendChild(blankSpan);
    return blankCell;
}

function createHtmlCheckboxFunction(checkBox) {
    return function () {
		checkBox.setAttribute("checked",checkBox.getAttribute("checked")=="false"?"true":"false");
		checkBox.setAttribute('class',(checkBox.attr("checked")=="true")?'checked':'unchecked');
	};
}

function createButtonCheckBox(id, checkedStyle, uncheckedStyle) {
	var enabled = checkedStyle;
	var disabled = uncheckedStyle;
	var cb = document.createElement("input");
	var sourceid = id;
	cb.setAttribute('checked','false');
	cb.setAttribute('class', disabled);
	cb.id = sourceid;
	cb.type = "button";
	cb.value = "";
	cb.setAttribute('checked','false');
	cb.onclick = function() {
		//var source = document.getElementById(sourceid);
		if (this.getAttribute('checked') == "true") {
			this.setAttribute('checked','false');
			this.setAttribute('class', disabled);
		} else {
			this.setAttribute('checked','true');
			this.setAttribute('class', enabled);
		}
	};
	return cb;
}
