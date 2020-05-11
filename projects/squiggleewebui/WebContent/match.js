// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
var matchJSON = new Object(); 
var currentTsData;
var pattern = [];
var matchResult = [];
var capturedPatterns;
var searchSelections = [];
var tsParameter, tsStartDate, tsEndDate, capturePatternStartDate, capturePatternName, capturePatternButton;
var viewStartDate, viewEndDate, tsGraphTitle;
var refreshChartViewFunc, refreshTsChartFunc;
var updateTsPanelButton, updateTsPanelButtonOn, addToSearchButton, addToSearchButtonOn, removeFromSearchButton, removeFromSearchButtonOn;
var updateMatchPanelButton, updateMatchPanelButtonOn;
var addTsToSearchButton, removeTsToSearchButton, searchTable;
var cb = [];
var tsViewStartts=-1, tsViewEndts=-1, tsViewStartHfOffset=-1, tsViewEndHfOffset=-1, currentFrequency = -1, currentTsData;
var animateImage, tstimerid, windowtimerid, animate = false, currentWindowStartts = -1, windowTsData = [], tsBuffer = [], fetchAheadWindows = 4, dataFetchInterval = 10000, animateInterval = 500; animateSize = 1;
var tsgraph, tsxaxis, tsyaxis, tsxaxisgroup, tsyaxisgroup, tspoints, tsaltxaxis, tsaltxaxisgroup, tsxtickgroup, tsaltxtickgroup, tsytickgroup, tsLineFunction;
var tsrow1labelgroup, tsrow2labelgroup, frequencyTitle;
var pgpoints, pggraph, pgxaxisgroup, pgyaxisgroup, pgyaxisgrouplabel, pgyaxisgrouplabeltext;
var mgpoints = [], mggraph = [], mgxaxisgroup = [], mgyaxisgroup = [], mgy1axisgroup = [], mggraphtitle = [], mggraphstart = [];
var imageControls_x = 600, imageControls_y = 75; imageControls_width = 16, imageControls_height = 16, imageControls_interval = 20;
var chartView_width = 355, chartView_height = 120, chartView_xloc = 475, chartView_yloc = 100;
var patternGraph_width = 100, patternGraph_height = 70, patternGraph_xloc = 270, patternGraph_yloc = 300;
var matchGraph_width = 70, matchGraph_height = 70;
var selectedChartIndex, selectionLayout;


function setMatchesView() {
	
	setRectangleView('Retrieval');
	for (var i=0; i< nodes.length; i++)
		nodes[i].on("click",createMatchesNodeSelectionFunction(i));
	
	if (typeof activeLn != 'undefined') {
		if (document.getElementById('patternLogicalNumberID') != null && typeof document.getElementById('patternLogicalNumberID').options != 'undefined')
			document.getElementById('patternLogicalNumberID').options.length = 0;
		var lnOptions = [];
		for (var i=0; i<clusterStatus.length; i++) {
			lnOptions[i] = clusterStatus[i].logicalNumber;
			if (clusterStatus[i].logicalNumber == activeLn) {
				nodes[i].attr("selected", "true");
	  			nodes[i].attr("rx", "" + selectedEllipse);
	  			nodes[i].attr("ry", "" + selectedEllipse);
			}
		}
		if (typeof document.getElementById('patternLogicalNumberID') != 'undefined')
			populateSelect('patternLogicalNumberID',lnOptions);
	}
	if (pageLoad) {
		setWindowSizes();
		setDimensionSizes();
		setControlPanel();
		setChartImageControls();
		addSelectionTable();
	}
	if (pageLoad)
		pageLoad = false;
}


function setControlPanel() {
	selectionLayout = svgContainer.append("foreignObject").attr("width", 370).attr("height", 450).attr("y",85).attr("x",(60))
	.html("<table width=\"360px\" id=\"selectionLayoutID\" ></table>");
	var layout = document.getElementById('selectionLayoutID');
	layout.setAttribute('class','selectiontablelayout');
	
	var row0 = layout.insertRow(0);
	row0.id = 'dataConfigLayoutRowID0';
	var row1 = layout.insertRow(1);
	row1.id = 'dataConfigLayoutRowID1';
	var row2 = layout.insertRow(2);
	row2.id = 'dataConfigLayoutRowID2';
	var row3 = layout.insertRow(3);
	row3.id = 'dataConfigLayoutRowID3';
	var row4 = layout.insertRow(4);
	row4.id = 'dataConfigLayoutRowID4';
	var row5 = layout.insertRow(5);
	row5.id = 'dataConfigLayoutRowID5';
	var row6 = layout.insertRow(6);
	row6.id = 'dataConfigLayoutRowID6';
	var row7 = layout.insertRow(7);
	row7.id = 'dataConfigLayoutRowID7';
	var row8 = layout.insertRow(8);
	row8.id = 'dataConfigLayoutRowID8';
	var row9 = layout.insertRow(9);
	row9.id = 'dataConfigLayoutRowID9';
	var row10 = layout.insertRow(10);
	row10.id = 'dataConfigLayoutRowID10';
	var row11 = layout.insertRow(11);
	row11.id = 'dataConfigLayoutRowID11';
	var row12 = layout.insertRow(12);
	row12.id = 'dataConfigLayoutRowID12';
	var row13 = layout.insertRow(13);
	row13.id = 'dataConfigLayoutRowID13';
	var row14 = layout.insertRow(14);
	row14.id = 'dataConfigLayoutRowID14';
	
    //var titleLabel = document.createElement("span");
    //titleLabel.innerHTML = "<p>Query Capture & Matching<p>";
    //titleLabel.setAttribute('class','dataconfigtabletitle');
    
    var tsIdLabel = document.createElement("span");
    tsIdLabel.innerHTML = "Time Series ID:";
    tsIdLabel.setAttribute('class','controllabel');
    var windowSizeLabel = document.createElement("span");
    windowSizeLabel.innerHTML = "Window Size:";
    windowSizeLabel.setAttribute('class','controllabel');
    var dimensionsLabel = document.createElement("span");
    dimensionsLabel.innerHTML = "Dimensions:";
    dimensionsLabel.setAttribute('class','controllabel');
    var tsStartLabel = document.createElement("span");
    tsStartLabel.innerHTML = "Start:";
    tsStartLabel.setAttribute('class','controllabel');
    var tsEndLabel = document.createElement("span");
    tsEndLabel.innerHTML = "End:";
    tsEndLabel.setAttribute('class','controllabel');
    
    var distanceLabel = document.createElement("span");
    distanceLabel.innerHTML = "Distance:";
    distanceLabel.setAttribute('class','controllabel');
    var datatypeLabel = document.createElement("span");
    datatypeLabel.innerHTML = "DataType:";1
    datatypeLabel.setAttribute('class','controllabel');
    var datatype = document.createElement("span");
    datatype.id = "datatypeID";
    datatype.innerHTML = "";
    var storedPatternLabel = document.createElement("span");
    storedPatternLabel.innerHTML = "Stored Patterns:";
    storedPatternLabel.setAttribute('class','controllabel');
    var xLabel = document.createElement("span");
    xLabel.innerHTML = "X";
    xLabel.setAttribute('class','controllabel');
    xLabel.id = "xLabelID";
    
    var patternNameLabel = document.createElement("span");
    patternNameLabel.innerHTML = "Pattern Name:";
    patternNameLabel.setAttribute('class','controllabel');
    
    var startBox = document.createElement("input");
    startBox.type = "text";
    startBox.size = 25;
    startBox.id = "tsStartDateID";
    
    var endBox = document.createElement("input");
    endBox.type = "text";
    endBox.size = 25;
    endBox.id = "tsEndDateID";
    
    var hfStartBox = document.createElement("input");
    hfStartBox.type = "text";
    hfStartBox.size = 25;
    hfStartBox.setAttribute('align','right');
    hfStartBox.id = "tsPanelStartHfOffsetID";
    
    var hfEndBox = document.createElement("input");
    hfEndBox.type = "text";
    hfEndBox.size = 25;
    hfEndBox.setAttribute('align','right');
    hfEndBox.id = "tsPanelEndHfOffsetID";
    
    var distanceBox = document.createElement("input");
    distanceBox.type = "text";
    distanceBox.size = 15;
    distanceBox.id = "matchPatternDistanceID";
    distanceBox.value = 2.0;
    
    var patternNameBox = document.createElement("input");
    patternNameBox.type = "text";
    patternNameBox.size = 25;
    patternNameBox.id = "patternNameID";
    
    var tsIdSelect = document.createElement("select");
    tsIdSelect.id = "tsParameterID";
    
    var dimensionsSelect = document.createElement("select");
    dimensionsSelect.id = "dimensionsID";
    
    var windowSizeSelect = document.createElement("select");
    windowSizeSelect.id = "tsWindowSizeID";
    
    var patternSelect = document.createElement("select");
    patternSelect.id = "matchPatternNameID";
    
	addToSearchButtonOn = function() {addToSearchList();};
    var addToSearchButton = document.createElement("input");
    addToSearchButton.type = "command";
    addToSearchButton.value = "Add";
    addToSearchButton.id = "addToSearchButtonID";
    addToSearchButton.readonly = true;
    addToSearchButton.setAttribute('class','cfButtonEnabled');
    addToSearchButton.onclick = addToSearchButtonOn;
    
    removeFromSearchButtonOn = function() {removeFromSearchList();};
    var removeFromSearchButton = document.createElement("input");
    removeFromSearchButton.type = "command";
    removeFromSearchButton.value = "Remove";
    removeFromSearchButton.id = "removeFromSearchButtonID";
    removeFromSearchButton.readonly = true;
    removeFromSearchButton.setAttribute('class','cfButtonEnabled');
    removeFromSearchButton.onclick = removeFromSearchButtonOn;
    
    updateMatchPanelButtonOn = function() {
		if (document.getElementById('matchPatternNameID').value == "" && matchJSON.tsResults.length == 0)
			return;
		
		if (activeLn || (activeLn === 0)) { 
			var webJson = new Object();
			var retrieveJSON = getPostBackParameters();
			retrieveJSON.ln = activeLn;
			retrieveJSON.action = "DR_MATCH";
			if (typeof matchJSON.pattern == 'undefined' || matchJSON.pattern.length == 0)
				return;
			if (!checkPatternSize(matchJSON.pattern.length)) {
				alert('Pattern size must match size of indexes on selected series');
				return;
			}
			
			retrieveJSON.pattern = matchJSON.pattern;
			
			retrieveJSON.tsResults = new Object();
			retrieveJSON.tsResults.data = [];
			retrieveJSON.matches = [];
			webJson.match = retrieveJSON;
			removeMatchGraphs();
			doAjax("/ajaxHandler", JSON.stringify(webJson), updateMatchCharts, reportError);
		}
	};
    var updateMatchPanelButton = document.createElement("input");
    updateMatchPanelButton.type = "command";
    updateMatchPanelButton.value = "Match";
    updateMatchPanelButton.id = "matchButtonID";
    updateMatchPanelButton.readonly = true;
    updateMatchPanelButton.setAttribute('class','cfButtonEnabled');
    updateMatchPanelButton.onclick = updateMatchPanelButtonOn;
    
    capturePatternButtonOn = function() {
		if (typeof activeLn != 'undefined' || (activeLn === 0)) { 
			var webJson = new Object();
			var retrieveJSON = getPostBackParameters();
			retrieveJSON.action = "DR_CAPTURE";
			retrieveJSON.ln = activeLn;
			retrievaJSON.cluster = currentCluster;
			retrieveJSON.tsResults = null;
			retrieveJSON.matches = [];
			retrieveJSON.selectedPattern = [];
			webJson.match = retrieveJSON;
			doAjax("/ajaxHandler", JSON.stringify(webJson), capturePattern, reportError);	// nothing to do, later refresh the list of captured patterns 
		}
	};
    var capturePatternButton = document.createElement("input");
    capturePatternButton.type = "command";
    capturePatternButton.value = "Capture";
    capturePatternButton.id = "captureButtonID";
    capturePatternButton.readonly = true;
    capturePatternButton.setAttribute('class','cfButtonEnabled');
    capturePatternButton.onclick = capturePatternButtonOn;
    
    //var td0 = document.createElement('td'); 
    //td0.colSpan = "6";
    //td0.appendChild(titleLabel);
    //row0.appendChild(td0);
    
    var td1 = document.createElement('td'); 
    td1.colSpan = "1";
    td1.appendChild(tsIdLabel);
    td1.setAttribute("text-align","left");
    row1.appendChild(td1);
    
    var td2 = document.createElement('td'); 
    td2.colSpan = "2";
    td2.appendChild(tsIdSelect);
    row1.appendChild(td2);
    
    var td3 = document.createElement('td'); 
    td3.colSpan = "1";
    td3.appendChild(windowSizeLabel);
    row1.appendChild(td3);
    
    var td4 = document.createElement('td'); 
    td4.colSpan = "2";
    td4.appendChild(windowSizeSelect);
    row1.appendChild(td4);
    
    row2.appendChild(getBlankColumn(6));
    
    var td5 = document.createElement('td'); 
    td5.colSpan = "1";
    td5.appendChild(dimensionsLabel);
    td5.setAttribute("text-align","left");
    row3.appendChild(td5);
    
    var td6 = document.createElement('td'); 
    td6.colSpan = "2";
    td6.appendChild(dimensionsSelect);
    row3.appendChild(td6);
    
    var td7 = document.createElement('td'); 
    td7.colSpan = "1";
    td7.appendChild(datatypeLabel);
    row3.appendChild(td7);
    
    var td8 = document.createElement('td'); 
    td8.colSpan = "2";
    td8.appendChild(datatype);
    row3.appendChild(td8);
    
    row4.appendChild(getBlankColumn(6));
    
    var td9 = document.createElement('td'); 
    td9.colSpan = "1";
    td9.appendChild(tsStartLabel);
    row5.appendChild(td9);
    
    var td10 = document.createElement('td'); 
    td10.colSpan = "2";
    td10.appendChild(startBox);
    td10.setAttribute("text-align","left");
    row5.appendChild(td10);
    
    var td11 = document.createElement('td'); 
    td11.colSpan = "1";
    td11.appendChild(tsEndLabel);
    row5.appendChild(td11);
    
    var td12 = document.createElement('td'); 
    td12.colSpan = "2";
    td12.appendChild(endBox);
    row5.appendChild(td12);
    
    //row6.appendChild(getBlankColumn(6));
    
    var td11hf = document.createElement('td'); 
    td11hf.colSpan = "3";
    td11hf.appendChild(hfStartBox);
    td11hf.setAttribute("align","right");
    row6.appendChild(td11hf);
    
    var td12hf = document.createElement('td'); 
    td12hf.colSpan = "3";
    td12hf.appendChild(hfEndBox);
    td12hf.setAttribute("align","right");
    row6.appendChild(td12hf);
    
    //row6.appendChild(getBlankColumn(1));
   
    var td13 = document.createElement('td'); 
    td13.colSpan = "1";
    td13.appendChild(storedPatternLabel);
    td13.setAttribute("text-align","left");
    row7.appendChild(td13);
    
    var td14 = document.createElement('td'); 
    td14.colSpan = "5";
    td14.appendChild(patternSelect);
    td14.appendChild(xLabel);
    row7.appendChild(td14);
    
    //var td15 = document.createElement('td'); 
    //td15.colSpan = "1";
    //td15.appendChild(xLabel);
    //td15.setAttribute("text-align","left");
    //row7.appendChild(td15);
    
    row8.appendChild(getBlankColumn(6));
    
    var td16 = document.createElement('td'); 
    td16.colSpan = "1";
    td16.appendChild(distanceLabel);
    td16.setAttribute("text-align","left");
    row9.appendChild(td16);
    
    var td17 = document.createElement('td'); 
    td17.colSpan = "2";
    td17.appendChild(distanceBox);
    row9.appendChild(td17);
    
    var td18 = document.createElement('td'); 
    td18.colSpan = "1";
    td18.appendChild(patternNameLabel);
    row9.appendChild(td18);
    
    var td19 = document.createElement('td'); 
    td19.colSpan = "2";
    td19.appendChild(patternNameBox);
    row9.appendChild(td19);
    
    row10.appendChild(getBlankColumn(6));
    row11.appendChild(getBlankColumn(6));
    
    var td20 = document.createElement('td'); 
    td20.colSpan = "6";
    td20.appendChild(addToSearchButton);
    td20.appendChild(getBlankSpan());
    td20.appendChild(getBlankSpan());
    td20.appendChild(getBlankSpan());
    td20.appendChild(getBlankSpan());
    td20.appendChild(getBlankSpan());
    td20.appendChild(removeFromSearchButton);
    td20.appendChild(getBlankSpan());
    td20.appendChild(getBlankSpan());
    td20.appendChild(getBlankSpan());
    td20.appendChild(getBlankSpan());
    td20.appendChild(getBlankSpan());
    td20.appendChild(updateMatchPanelButton);
    td20.appendChild(getBlankSpan());
    td20.appendChild(getBlankSpan());
    td20.appendChild(getBlankSpan());
    td20.appendChild(getBlankSpan());
    td20.appendChild(getBlankSpan());
    td20.appendChild(capturePatternButton);
    row12.appendChild(td20);
    
    row13.appendChild(getBlankColumn(6));
    
    searchTable = document.createElement("table");
    searchTable.width = "100%";
    searchTable.id = "searchTableID";
    //searchTable.setAttribute('style','border: 1px solid gainsboro');
    searchTable.setAttribute('class','selectiontable');
    
    var td21 = document.createElement('td'); 
    td21.colSpan = "6";
    td21.appendChild(searchTable);
    row14.appendChild(td21);
    
    document.getElementById('matchPatternNameID').onchange = function() {fetchPattern();};
    
    windowSizeSelect.onchange = function() {
		tsViewStartts = getConfigValue(document.getElementById('tsParameterID').value, nodeConfig, 'startts');
		tsViewEndts = getConfigValue(document.getElementById('tsParameterID').value, nodeConfig, 'endts');
		
		selectedChartIndex = -1;
		window["refreshChartViewFunc"]();
	};
	
	dimensionsSelect.onchange = function() {
		tsViewStartts = getConfigValue(document.getElementById('tsParameterID').value, nodeConfig, 'startts');
		tsViewEndts = getConfigValue(document.getElementById('tsParameterID').value, nodeConfig, 'endts');
		selectedChartIndex = -1;
		window["refreshChartViewFunc"]();
	};
	
	document.getElementById('tsParameterID').onchange = function() {
		var selectedParameter = document.getElementById('tsParameterID').value;
		document.getElementById('tsStartDateID').value = getConfigValue(document.getElementById('tsParameterID').value, nodeConfig, 'start');
		document.getElementById('tsEndDateID').value = getConfigValue(selectedParameter, nodeConfig, 'end');
		if (typeof document.getElementById('datatypeID') != 'undefined')
			document.getElementById('datatypeID').innerText = getConfigValue(selectedParameter, nodeConfig, 'datatype');
		populateSelect('matchPatternNameID', getCapturedPatternNames(getConfigValue(selectedParameter, nodeConfig, 'datatype')));
		tsViewStartts = getConfigValue(document.getElementById('tsParameterID').value, nodeConfig, 'startts');
		tsViewEndts = getConfigValue(document.getElementById('tsParameterID').value, nodeConfig, 'endts');
		if (getConfigValue(document.getElementById('tsParameterID').value, nodeConfig, 'isPatternIndexed') == "true")
			document.getElementById('dimensionsID').value = getConfigValue(document.getElementById('tsParameterID').value, nodeConfig, 'patternIndexSize');
		currentFrequency = getConfigValue(selectedParameter, nodeConfig, 'frequency');
		if (currentFrequency == 'MICROS' || currentFrequency == 'NANOS') {
			document.getElementById('tsPanelStartHfOffsetID').style.display = 'inline';
			document.getElementById('tsPanelEndHfOffsetID').style.display = 'inline';
			document.getElementById('tsPanelStartHfOffsetID').value = 0;
			document.getElementById('tsPanelEndHfOffsetID').value = ((currentFrequency == 'MICROS')?999:999999);
		} else {
			document.getElementById('tsPanelStartHfOffsetID').style.display = 'none';
			document.getElementById('tsPanelEndHfOffsetID').style.display = 'none';
			document.getElementById('tsPanelStartHfOffsetID').value = -1;
			document.getElementById('tsPanelEndHfOffsetID').value = -1;
		}
		tsViewStartHfOffset = -1;
		tsViewEndHfOffset = -1;
		if (typeof tstimerid != 'undefined')
			clearInterval(tstimerid);
		if (typeof windowtimerid != 'undefined')
			clearInterval(windowtimerid);
		windowTsData = [];
		tsBuffer = [];
		animate = false;
		
		currentWindowStartts = -1;
		selectedChartIndex = -1;
		fetchPattern();
		window["refreshChartViewFunc"]();
	};
	
	document.getElementById('xLabelID').onclick = function() {
		document.getElementById('matchPatternNameID').selectedIndex = -1;
		matchJSON.pattern = [];
	};
}


function setChartImageControls() {
	var offset = 0;
	svgContainer.append("svg:image").attr("x",(imageControls_x + offset++*imageControls_interval)).attr("y",imageControls_y).attr("xlink:href","images/tab_first_16.png")
		.attr('width', imageControls_width).attr('height', imageControls_height)
		.on("click", function() {
		if (activeLn || (activeLn === 0)) { 
			tsViewStartts = -1;
			tsViewEndts = -1;
			tsViewStartHfOffset = -1;
			tsViewEndHfOffset = -1;
			selectedChartIndex = -1;
			
			if (typeof tstimerid != 'undefined')
				clearInterval(tstimerid);
			if (typeof windowtimerid != 'undefined')
				clearInterval(windowtimerid);
			windowTsData = [];
			tsBuffer = [];
			animate = false;
			animateImage.attr("xlink:href","images/media_playback_start.png");
			window["refreshChartViewFunc"](false);
		}
		});
	svgContainer.append("svg:image").attr("x",(imageControls_x + offset++*imageControls_interval)).attr("y",imageControls_y).attr("xlink:href","images/tab_left_16.png")
		.attr('width', imageControls_width).attr('height', imageControls_height)
		.on("click", function() {
		if (activeLn || (activeLn === 0)) { 
			if (currentFrequency == 'MICROS') {
				tsViewEndHfOffset = tsViewStartHfOffset - 1;
				if (tsViewEndHfOffset < 0) {
					tsViewEndts = tsViewStartts - 1;
					var dt = new Date();
					dt.setTime(tsViewEndts);
					tsViewEndHfOffset += (dt.getUTCMilliseconds()*1000 + 999);
				}
			}
			else if (currentFrequency == 'NANOS') {
				tsViewEndHfOffset = tsViewStartHfOffset - 1;
				if (tsViewEndHfOffset < 0) {
					tsViewEndts = tsViewStartts - 1;
					var dt = new Date();
					dt.setTime(tsViewEndts);
					tsViewEndHfOffset += (dt.getUTCMilliseconds()*1000000 + 999999);
				}
			}
			else {
				tsViewEndts = tsViewStartts - 1;
			}
			tsViewStartts = -1;
			tsViewStartHfOffset  = -1;
			selectedChartIndex = -1;
			
			if (typeof tstimerid != 'undefined')
				clearInterval(tstimerid);
			if (typeof windowtimerid != 'undefined')
				clearInterval(windowtimerid);
			windowTsData = [];
			tsBuffer = [];
			animate = false;
			animateImage.attr("xlink:href","images/media_playback_start.png");
			window["refreshChartViewFunc"](true);
		}
		});
	
	refreshChartViewFunc = function(last) {
		//timerId = setInterval(function () {animate();}, animateFetchInterval);
		if (activeLn || (activeLn === 0)) { 
			if (typeof last == 'undefined')
				last = true;
			var webJson = new Object();
			var retrieveJSON = getPostBackParameters();
			retrieveJSON.ln = activeLn;
			retrieveJSON.cluster = currentCluster;
			retrieveJSON.action = "DR_UPDATE_TS";
			if (typeof retrieveJSON.tsResults != 'undefined')
				retrieveJSON.tsResults.data = [];
			retrieveJSON.matches = [];
			retrieveJSON.selectedPattern = [];
			retrieveJSON.capturePattern = [];
			retrieveJSON.last = (last?"true":"false");
			webJson.match = retrieveJSON;
			selectedChartIndex = -1;
			
			if (typeof tstimerid != 'undefined')
				clearInterval(tstimerid);
			if (typeof windowtimerid != 'undefined')
				clearInterval(windowtimerid);
			windowTsData = [];
			tsBuffer = [];
			animate = false;
			animateImage.attr("xlink:href","images/media_playback_start.png");
			doAjax("/ajaxHandler", JSON.stringify(webJson), updateTsChart, reportError);
		}
	};
	
	svgContainer.append("svg:image").attr("x",(imageControls_x + offset++*imageControls_interval)).attr("y",imageControls_y).attr("xlink:href","images/refresh_16.png")
		.attr('width', imageControls_width).attr('height', imageControls_height)
		.on("click", function() {
			selectedChartIndex = -1;
			
			if (typeof tstimerid != 'undefined')
				clearInterval(tstimerid);
			if (typeof windowtimerid != 'undefined')
				clearInterval(windowtimerid);
			windowTsData = [];
			tsBuffer = [];
			animate = false;
			animateImage.attr("xlink:href","images/media_playback_start.png");
			window["refreshChartViewFunc"](false);
		});
	
	svgContainer.append("svg:image").attr("x",(imageControls_x + offset++*imageControls_interval)).attr("y",imageControls_y).attr("xlink:href","images/tab_right_16.png")
		.attr('width', imageControls_width).attr('height', imageControls_height)
		.on("click", function() {
		if (activeLn || (activeLn === 0)) { 
			if (currentFrequency == 'MICROS') {
				tsViewStartHfOffset = tsViewEndHfOffset + 1;
				tsViewEndHfOffset  = -1;
				if (tsViewStartHfOffset > 999) {
					var dt = new Date();
					dt.setTime(tsViewStartts);
					tsViewStartHfOffset -= (dt.getUTCMilliseconds()*1000000 + 999);
					tsViewEndts = -1;
				}
			}
			else if (currentFrequency == 'NANOS') {
				tsViewStartHfOffset = tsViewEndHfOffset + 1;
				tsViewStartHfOffset  = -1;
				if (tsViewEndHfOffset > 999999) {
					tsViewStartts = tsViewEndts + 1;
					var dt = new Date();
					dt.setTime(tsViewStartts);
					tsViewStartHfOffset -= (dt.getUTCMilliseconds()*1000000 + 999999);
					tsViewEndts = -1;
				}
			}
			else {
				tsViewStartts = tsViewEndts + 1;
				tsViewEndts = -1;
			}
			selectedChartIndex = -1;
			
			if (typeof tstimerid != 'undefined')
				clearInterval(tstimerid);
			if (typeof windowtimerid != 'undefined')
				clearInterval(windowtimerid);
			windowTsData = [];
			tsBuffer = [];
			animate = false;
			animateImage.attr("xlink:href","images/media_playback_start.png");
			window["refreshChartViewFunc"](false);
		}
		});
	svgContainer.append("svg:image").attr("x",(imageControls_x + offset++*imageControls_interval)).attr("y",imageControls_y).attr("xlink:href","images/tab_last_16.png")
		.attr('width', imageControls_width).attr('height', imageControls_height)
		.on("click", function() {
			if (activeLn || (activeLn === 0)) { 
				tsViewStartts = -1;
				tsViewEndts = -1;
				tsViewStartHfOffset = -1;
				tsViewEndHfOffset = -1;
				selectedChartIndex = -1;
				
				if (typeof tstimerid != 'undefined')
					clearInterval(tstimerid);
				if (typeof windowtimerid != 'undefined')
					clearInterval(windowtimerid);
				windowTsData = [];
				tsBuffer = [];
				animate = false;
				animateImage.attr("xlink:href","images/media_playback_start.png");
				window["refreshChartViewFunc"](true);
			}
			});
	
	fetchAheadFunc = function(last) {
		if (activeLn || (activeLn === 0)) { 
			if (typeof last == 'undefined')
				last = true;
			var webJson = new Object();
			var retrieveJSON = getPostBackParameters();
			selectedChartIndex = -1;
			retrieveJSON.ln = activeLn;
			retrieveJSON.cluster = currentCluster;
			retrieveJSON.action = "DR_FETCH_AHEAD";
			
			retrieveJSON.matches = [];
			retrieveJSON.selectedPattern = [];
			retrieveJSON.capturePattern = [];
			retrieveJSON.last = (last?"true":"false");
			retrieveJSON.tsWindowSize = +fetchAheadWindows*+retrieveJSON.tsWindowSize;
			if (tsBuffer.length > retrieveJSON.tsWindowSize)
				return;
			var dt = new Date();
			dt.setTime(+currentWindowStartts);
			if (tsBuffer.length > 0) {
				if (currentFrequency == 'MICROS') {
					//tsViewStartHfOffset = tsViewEndHfOffset + 1;
					retrieveJSON.tsViewStartHfOffset = +tsBuffer[tsBuffer.length - 1].offset + 1;
					if (+retrieveJSON.tsViewStartHfOffset % 999 > 0) {
						retrieveJSON.tsViewStartHfOffset = +retrieveJSON.tsViewStartHfOffset % 999;
						retrieveJSON.tsViewStartts = advance(currentFrequency, dt, 1).getTime();
					} else
						retrieveJSON.tsViewStartts = dt.getTime();
					retrieveJSON.tsViewEndts  = -1;
					retrieveJSON.tsViewEndHfOffset  = -1;
				}
				else if (currentFrequency == 'NANOS') {
					retrieveJSON.tsViewStartHfOffset = +tsBuffer[tsBuffer.length - 1].offset + 1;
					if (+retrieveJSON.tsViewStartHfOffset % 999999 > 0) {
						retrieveJSON.tsViewStartHfOffset = +retrieveJSON.tsViewStartHfOffset % 999999;
						retrieveJSON.tsViewStartts = advance(currentFrequency, dt, 1).getTime();
					} else
						retrieveJSON.tsViewStartts = dt.getTime();
					retrieveJSON.tsViewEndts  = -1;
					retrieveJSON.tsViewEndHfOffset  = -1;
				}
				else {
					retrieveJSON.tsViewStartts = advance(currentFrequency, dt, (+tsBuffer[tsBuffer.length - 1].offset + 1)).getTime();
					retrieveJSON.tsViewEndts = -1;
				}
			} else if (retrieveJSON.tsResults.data.length > 0) {
				if (currentFrequency == 'MICROS') {
					retrieveJSON.tsViewStartHfOffset = +retrieveJSON.tsViewEndHfOffset + 1;
					if (+retrieveJSON.tsViewStartHfOffset % 999 > 0) {
						retrieveJSON.tsViewStartHfOffset = +retrieveJSON.tsViewStartHfOffset % 999;
						retrieveJSON.tsViewStartts = advance(currentFrequency, dt, 1).getTime();
					} else
						retrieveJSON.tsViewStartts = dt.getTime();
					retrieveJSON.tsViewEndts  = -1;
					retrieveJSON.tsViewEndHfOffset  = -1;
				}
				else if (currentFrequency == 'NANOS') {
					retrieveJSON.tsViewStartHfOffset = +retrieveJSON.tsViewEndHfOffset + 1;
					if (+retrieveJSON.tsViewStartHfOffset % 999999 > 0) {
						retrieveJSON.tsViewStartHfOffset = +retrieveJSON.tsViewStartHfOffset % 999999;
						retrieveJSON.tsViewStartts = advance(currentFrequency, dt, 1).getTime();
					} else
						retrieveJSON.tsViewStartts = dt.getTime();
					retrieveJSON.tsViewEndts  = -1;
					retrieveJSON.tsViewEndHfOffset  = -1;
				}
				else {
					retrieveJSON.tsViewStartts = +retrieveJSON.tsViewEndts + 1;
					retrieveJSON.tsViewEndts = -1;
				}
			} else {
				retrieveJSON.tsViewStartts = -1;
				retrieveJSON.tsViewEndts = -1;
				retrieveJSON.tsViewStartHfOffset = -1;
				retrieveJSON.tsViewEndHfOffset = -1;
			}
			
			if (typeof retrieveJSON.tsResults != 'undefined')
				retrieveJSON.tsResults.data = [];
			webJson.match = retrieveJSON;
			doAjax("/ajaxHandler", JSON.stringify(webJson), fetchAheadResult, reportError);
		}
	};

	animateImage = svgContainer.append("svg:image").attr("x",(imageControls_x + offset++*imageControls_interval)).attr("y",imageControls_y).attr("xlink:href","images/media_playback_start.png")
	.attr('width', imageControls_width).attr('height', imageControls_height)
	.on("click", function() {
			if (activeLn || (activeLn === 0)) { 
				if (typeof windowTsData == 'undefined' || windowTsData.length <= 0) {
					if (typeof matchJSON != 'undefined' && typeof matchJSON.tsResults != 'undefined' && typeof matchJSON.tsResults.data != 'undefined' && matchJSON.tsResults.data.length > 0) {
						windowTsData = matchJSON.tsResults.data;
						currentWindowStartts = matchJSON.tsResults.startts;
					}
					else {
						windowTsData = [];
						currentWindowStartts = -1;
					}
				}
				animate = true;
				animateImage.attr("xlink:href","images/media_playback_start_selected.png");
				fetchAheadFunc(false);
			}
			});
	svgContainer.append("svg:image").attr("x",(imageControls_x + offset++*imageControls_interval)).attr("y",imageControls_y).attr("xlink:href","images/media_playback_stop1.png")
	.attr('width', imageControls_width).attr('height', imageControls_height)
	.on("click", function() {
		if (activeLn || (activeLn === 0)) { 
			animate = false;
			animateImage.attr("xlink:href","images/media_playback_start.png");
			tsBuffer = [];
			if (typeof tstimerid != 'undefined')
				clearInterval(tstimerid); 
			if (typeof windowtimerid != 'undefined')
				clearInterval(windowtimerid);
			
			if (currentFrequency == 'MICROS' || currentFrequency == 'NANOS') {
				tsViewStartts = +currentWindowStartts;
				tsViewEndts = +currentWindowStartts;
				tsViewStartHfOffset = +windowTsData[0].offset;
				tsViewEndHfOffset = +windowTsData[windowTsData.length - 1].offset;
			}
			else {
				tsViewStartts = +currentWindowStartts;
				var dt = new Date();
				dt.setTime(tsViewStartts);
				//tsViewStartts = advance(currentFrequency, dt, +windowTsData[0].offset).getTime();
				tsViewEndts = advance(currentFrequency, dt, +windowTsData[windowTsData.length - 1].offset).getTime();
			}
		}
	});

}


function capturePattern(result, textStatus, jQxhr) {
	var ajaxJson = JSON.parse(result);
	//matchJSON.tsResults = ajaxJson.tsResults;
	notificationPanel.text("Data capture action completed for pattern: " + ajaxJson.match.tsPanelPatternName); 
	messagePanel.text("OK");
}


function updateTsChart(result, textStatus, jQxhr) {
	var ajaxJson = JSON.parse(result);
	matchJSON.tsResults = ajaxJson.match.tsResults;
	tsViewStartts = +ajaxJson.match.tsViewStartts;
	tsViewEndts = +ajaxJson.match.tsViewEndts;
	tsViewStartHfOffset = +ajaxJson.match.tsViewStartHfOffset;
	tsViewEndHfOffset = +ajaxJson.match.tsViewEndHfOffset;
	setChartView(matchJSON.tsResults.data, matchJSON.tsResults.startts);
	messagePanel.text("OK");
}

function fetchAheadResult(result, textStatus, jQxhr) {
	if (typeof windowtimerid != 'undefined')
		clearInterval(windowtimerid);
	
	var ajaxJson = JSON.parse(result);
	if (typeof ajaxJson.match.tsResults.data == 'undefined' || ajaxJson.match.tsResults.data.length == 0)
		return;
	var oldWindowStartts = +currentWindowStartts;
	currentWindowStartts = +ajaxJson.match.tsResults.startts;
	var dt1 = new Date(), dt2 = new Date(), trsteps = 0;
	var multiplier = 1;
	if (currentFrequency == 'MICROS')
		multiplier = 1000;
	if (currentFrequency == 'NANOS')
		multiplier = 1000000;
	if (windowTsData.length > 0) {
		dt1.setTime(oldWindowStartts);
		dt2.setTime(currentWindowStartts);
		trsteps = steps(currentFrequency, dt1, dt2);
	}
	for (var i=0; i<windowTsData.length; i++) {
		windowTsData[i].offset = +windowTsData[i].offset - trsteps*multiplier;
		//windowTsData[i].offset = (+windowTsData[i].offset - currentWindowStartts + oldWindowStartts) + "";
	}

	//if (tsBuffer.length > 0) {
	//	dt1.setTime(oldWindowStartts);
	//	dt2.setTime(currentWindowStartts);
	//	trsteps = steps(currentFrequency, dt1, dt2);
	//}
	for (var i=0; i<tsBuffer.length; i++) {
		//dt1.setTime(oldWindowStartts + +tsBuffer[i].offset);
		//dt2.setTime(currentWindowStartts);
		//trsteps = steps(currentFrequency, dt1, dt2);
		tsBuffer[i].offset = +tsBuffer[i].offset - trsteps*multiplier;
		//tsBuffer[i].offset = (+tsBuffer[i].offset - currentWindowStartts + oldWindowStartts) + "";
	}
	
	for (var i=0; i< ajaxJson.match.tsResults.data.length; i++)
		tsBuffer.push(ajaxJson.match.tsResults.data[i]);
	
	while (windowTsData.length < (+document.getElementById('tsWindowSizeID').value)) {
		if (tsBuffer.length == 0)
			break;
		windowTsData.push(tsBuffer[0]);
		tsBuffer.shift();
	}
	
	windowtimerid = setInterval(function() {animateChart();},animateInterval);
	if (typeof tstimerid == 'undefined')
		tstimerid = setInterval(function() {fetchAheadFunc(false);}, dataFetchInterval);
}

function animateChart() {
	var winsize = +document.getElementById('tsWindowSizeID').value;
	if (typeof windowTsData != 'undefined' && typeof tsBuffer != 'undefined' && tsBuffer.length > 0) {
		windowTsData.push(tsBuffer[0]);
		tsBuffer.shift();
		
		if (windowTsData.length > winsize)
			windowTsData.shift();
		/*
		if (currentFrequency == 'MICROS' || currentFrequency == 'NANOS') {
			tsViewStartts = +currentWindowStartts;
			tsViewEndts = +currentWindowStartts;
			tsViewStartHfOffset = +windowTsData[0].offset;
			tsViewEndHfOffset = +windowTsData[windowTsData.length - 1].offset;
		}
		else {
			tsViewStartts = +currentWindowStartts + +windowTsData[0].offset;
			tsViewEndts = +currentWindowStartts + +windowTsData[windowTsData.length - 1].offset;
		}
		*/
	}
	setChartView(windowTsData, +currentWindowStartts);
	
	//if (animate) 
	//	setTimeout(function() {animateChart();},animateInterval);
}


function getPostBackParameters() {
	matchJSON.tsPanelDimensionality = document.getElementById('dimensionsID').value;
	matchJSON.tsPanelParameter = document.getElementById('tsParameterID').value;
	matchJSON.tsPanelPatternName = document.getElementById('patternNameID').value;
	matchJSON.tsPanelStart = document.getElementById('tsStartDateID').value;
	matchJSON.tsPanelEnd = document.getElementById('tsEndDateID').value;
	matchJSON.tsPanelStartHfOffset = document.getElementById('tsPanelStartHfOffsetID').value;
	matchJSON.tsPanelEndHfOffset = document.getElementById('tsPanelEndHfOffsetID').value;
	matchJSON.tsViewStartHfOffset = tsViewStartHfOffset + '';
	matchJSON.tsViewEndHfOffset = tsViewEndHfOffset + '';
	matchJSON.tsWindowSize = document.getElementById('tsWindowSizeID').value;
	matchJSON.tsViewStartts = tsViewStartts + '';
	matchJSON.tsViewEndts = tsViewEndts + '';
	matchJSON.frequency = currentFrequency + '';
	matchJSON.cluster = currentCluster;
	matchJSON.dataType = document.getElementById('datatypeID').innerText;
	matchJSON.matchDistance = document.getElementById('matchPatternDistanceID').value;
	matchJSON.ptrnPanelPatternName = document.getElementById('matchPatternNameID').value;
	
	matchJSON.searchSelections = searchSelections;
	for (var i=0; i< matchJSON.searchSelections; i++)
		matchJSON.searchSelections[i].index = null;
	
	return JSON.parse(JSON.stringify(matchJSON));	//clone the object
	//matchJSON.tsResults = []; //do not postback the fetched data
	//matchJSON.matches = []; //do not postback the matches
}

function updateMatchCharts(result, textStatus, jQxhr) {
	var ajaxJson = JSON.parse(result);
	matchJSON.matches = ajaxJson.match.matches;
	
	//if (matchJSON.pattern && matchJSON.pattern.length > 0)
	//	setPatternGraph(matchJSON.pattern);
	//alert(matchJSON.matches.length);
	removeMatchGraphs();
	if (matchJSON.matches && matchJSON.matches.length > 0 && matchJSON.pattern && matchJSON.pattern.length > 0) {
		var rowIndex, colIndex;
		// hardwired for a panel of graphs with 2 rows and 3 columns
		for (var i =0; i < matchJSON.matches.length; i++) {
			if (i < 3) {
				rowIndex = 0;
				colIndex = i;
			} else {
				rowIndex = 1;
				colIndex = (i-3);
			}
			setMatchGraph(matchJSON.pattern,matchJSON.matches[i],matchGraph_width,matchGraph_height, (465 + colIndex*140),(319 + rowIndex*125), i);	
		}
	}
	matchJSON.matches = [];
	messagePanel.text("OK");
} 


function fetchPattern() {
	if (activeLn || (activeLn === 0)) { 
		var webJson = new Object();
		var retrieveJSON = getPostBackParameters();
		retrieveJSON.ln = activeLn;
		retrieveJSON.cluster = currentCluster;
		retrieveJSON.action = "DR_FETCH_PATTERN";
		retrieveJSON.tsResults = new Object();
		retrieveJSON.tsResults.data = [];
		retrieveJSON.matches = [];
		webJson.match = retrieveJSON;
		doAjax("/ajaxHandler", JSON.stringify(webJson), fetchPatternUpdate, reportError);
	}
}


function fetchPatternUpdate(result, textStatus, jQxhr) {
	var ajaxJson = JSON.parse(result);
	matchJSON.pattern = ajaxJson.match.pattern;
	//if (matchJSON.pattern && matchJSON.pattern.length > 0)
	//	setPatternGraph(matchJSON.pattern);
}


function addToSearchList() {
	if (typeof activeLn == 'undefined') {
		return;
	}
	
	if (!checkDataType(document.getElementById('datatypeID').innerHTML)) {
		alert ("All time series selected for matching must have the same data type");
		return;
	}
	
	if (!checkDimensions(document.getElementById('dimensionsID').value)) {
		alert ("All time series selected for matching must have the same dimensions");
		return;
	}
	
	var addition = new Object();
	addition.index = searchSelections.length;
	addition.cluster = currentCluster;
	addition.ln = activeLn;
	addition.parameter = document.getElementById('tsParameterID').value;
	addition.datatype = document.getElementById('datatypeID').innerText;
	addition.start = document.getElementById('tsStartDateID').value;
	addition.end = document.getElementById('tsEndDateID').value;
	addition.dimensions = document.getElementById('dimensionsID').value;
	var added = false;
	
	for (var i=0; i< searchSelections.length; i++) {
		if (searchSelections[i].ln == addition.ln && 
			searchSelections[i].cluster == addition.cluster && 
			searchSelections[i].parameter == addition.parameter && 
			searchSelections[i].start == addition.start && 
			searchSelections[i].end == addition.end && 
			searchSelections[i].dimensions == addition.dimensions) {
			added = true;
			//alert('already added this selection for search!');
			break;
		}
	}
	
	if (!added)
		searchSelections[searchSelections.length] = addition;
	
	matchJSON.searchSelections = searchSelections;
	addSelectionTable();
}

function removeFromSearchList() {
	if (typeof activeLn == 'undefined' || typeof cb == 'undefined')
		return;
	
	for (var i=0; i< cb.length; i++) {
		if (cb[i].getAttribute('checked') == 'true') {
			for (var j=0; j<searchSelections.length; j++) {
				if (searchSelections[j].index == cb[i].getAttribute("index"))
					searchSelections.splice(j,1);
			}
		}
	}
	
	for (var j=0; j<searchSelections.length; j++) {
		searchSelections[j].index = j;
	}
	matchJSON.searchSelections = searchSelections;
	addSelectionTable();
}

function checkDataType(datatype) {
	for (var i=0; i< searchSelections.length; i++)
		if (searchSelections[i].datatype != datatype)
			return false;
	
	return true;
}

function checkDimensions(dimensions) {
	for (var i=0; i< searchSelections.length; i++)
		if (searchSelections[i].dimensions != dimensions)
			return false;
	
	return true;
}

function checkPatternSize(psize) {
	if (typeof searchSelections == 'undefined' || searchSelections/length == 0)
		return false;
	if (+searchSelections[0].dimensions == +psize) {
		return true;
	}
	
	return false;
}

function addSelectionTable() {
	if (typeof searchTable != 'undefined')
		searchTable.innerHTML = "";
	
	var htmlTable = document.getElementById('searchTableID');
	htmlTable.setAttribute('class','selectiontable');
	//htmlTable.setAttribute('style','border: 1px solid gainsboro');
    var htmlHeader = document.createElement("thead");
    htmlHeader.className = 'selectiontableheader';
    htmlHeader.id = 'searchTableHeaderID';
    htmlTable.appendChild(htmlHeader);
    var th1 = document.createElement('th'); 
    th1.innerHTML = "";
    th1.setAttribute('class','selectiontablecell');
    htmlHeader.appendChild(th1);
    th1.width = '10px';
    var th2 = document.createElement('th'); 
    th2.innerHTML = "Ln";
    th2.setAttribute('class','selectiontablecell');
    htmlHeader.appendChild(th2);
    th2.width = '10px';
    var th3 = document.createElement('th'); 
    th3.innerHTML = "Id";
    th3.setAttribute('class','selectiontablecell');
    htmlHeader.appendChild(th3);
    th3.width = '65px';
    var th4 = document.createElement('th'); 
    th4.innerHTML = "Start";
    th4.setAttribute('class','selectiontablecell');
    htmlHeader.appendChild(th4);
    th4.width = '80px';
    var th5 = document.createElement('th'); 
    th5.innerHTML = "End";
    th5.setAttribute('class','selectiontablecell');
    htmlHeader.appendChild(th5);
    th5.width = '80px';
	
	for (var i=0; i< searchSelections.length; i++)
		addTableRow(i,searchSelections[i].ln,searchSelections[i].parameter,searchSelections[i].start,searchSelections[i].end);
}

function addTableRow(index, ln, id, start, end) {
	var htmlTable = document.getElementById('searchTableID');
	
	var row = htmlTable.insertRow(index);
    row.id = 'searchTableRowID' + index;
    htmlTable.appendChild(row);
    if (index % 2 == 0)
    	row.className = 'selectiontablerow';
    else
    	row.className = 'selectiontablerowalt';
    	
    
    var th1 = document.createElement('td'); 
    //cb[index] = document.createElement('input');
    cb[index] = createButtonCheckBox('cbID' + index, 'rowenabled', 'rowdisabled');
    //cb[index].type = 'checkbox';
    cb[index].setAttribute("index",'' + index);
    //cb[rowIndex].onchange = createCheckboxFunction(cb[index]);
    //if (selected)
    //	cb[rowIndex].checked = true;
    th1.appendChild(cb[index]);
    th1.className = 'selectiontablecell';
    row.appendChild(th1);
    
    var th2 = document.createElement('td'); 
    th2.innerHTML = ln;
    th2.className = 'selectiontablecell';
    row.appendChild(th2);
    
    var th3 = document.createElement('td'); 
    th3.innerHTML = id;
    th3.className = 'selectiontablecell';
    row.appendChild(th3);

    var th4 = document.createElement('td'); 
    th4.innerHTML = start;
    th4.className = 'selectiontablecell';
    row.appendChild(th4);

    var th5 = document.createElement('td'); 
    th5.innerHTML = end;
    th5.className = 'selectiontablecell';
    row.appendChild(th5);
}


function clearChartView() {
	if (typeof tsgraph != 'undefined')
		tsgraph.remove();
	if (typeof tsxaxisgroup != 'undefined')
		tsxaxisgroup.remove();
	if (typeof tsxaxis != 'undefined')
		tsxaxis = null;	
	if (typeof tsaltxaxisgroup != 'undefined')
		tsaltxaxisgroup.remove();
	if (typeof tsaltxaxis != 'undefined')
		tsaltxaxis = null;
	if (typeof tsyaxisgroup != 'undefined')
		tsyaxisgroup.remove();
	if (typeof tsyaxis != 'undefined')
		tsyaxis = null;
	if (typeof tspoints != 'undefined')
		tspoints.remove();
	if (typeof tsGraphTitle != 'undefined')
		tsGraphTitle.remove();
	if (typeof tsxtickgroup != 'undefined')
		tsxtickgroup.remove();
	if (typeof tsytickgroup != 'undefined')
		tsytickgroup.remove();
	if (typeof tsaltxtickgroup != 'undefined')
		tsaltxtickgroup.remove();
	if (typeof tsrow1labelgroup != 'undefined')
		tsrow1labelgroup.remove();
	if (typeof tsrow2labelgroup != 'undefined')
		tsrow2labelgroup.remove();
	if (typeof frequencyTitle != 'undefined')
		frequencyTitle.remove();
}

function setChartView(data, startts) {
	var nxticks = 5;
	var naltxticks = 10;
	var nyticks = 5;
	var altxaxisoffset = 30;
	var width = chartView_width;
	var height = chartView_height;
	var xloc = chartView_xloc;
	var yloc = chartView_yloc;
	
	clearChartView();
	
	if (typeof data == 'undefined' || typeof data.length == 'undefined' || data.length == 0)
		return;
	var isHf = false;
	if (currentFrequency == 'MICROS' || currentFrequency == 'NANOS')
		isHf = true;
	var tsaltxrange = d3.scale.linear().range([0, width]);
	var tshfxrange = d3.scale.linear().range([0, width]);
	var tsyrange = d3.scale.linear().range([height, 0]);
	tsyaxis = d3.svg.axis().scale(tsyrange).orient("left").ticks((data.length > nyticks)?nyticks:data.length);
	var startDate = new Date();
	startDate.setTime(+startts);
	var startoffset = parseInt(+data[0].offset);
	var minDate = advance(currentFrequency, startDate, startoffset);
	var maxDate = advance(currentFrequency, startDate, parseInt(+data[data.length - 1].offset));
	var tscale = d3.time.scale();
	tscale.range([0, width]);
	tscale.domain([minDate, maxDate]);
	tsxaxis = d3.svg.axis().scale(((isHf)?tshfxrange:tscale)).orient("bottom").ticks((data.length > nxticks)?nxticks:data.length)
		.tickFormat(( (isHf) ? (d3.format(",.0f")) : (getTickFormat(currentFrequency)) ) );
	tsaltxaxis = d3.svg.axis().scale(tsaltxrange).orient("bottom").ticks((data.length > naltxticks)?naltxticks:data.length);
	tsaltxrange.domain(d3.extent(data, function(d,i) { return i; }));
	tshfxrange.domain(d3.extent(data, function(d,i) { return +d.offset; }));
	tsyrange.domain(d3.extent(data, function(d,i) { return +d.val; }));
	
	//svgContainer.append("defs").append("clipPath").attr("id", "clip").append("rect").attr("width", width).attr("height", height).attr("x",+xloc).attr("y",yloc);
	
	tsLineFunction = d3.svg.line()
		.x(function (d,i) {	
			if (isHf) {
				return tshfxrange(parseInt(+d.offset));
			} else
				return tscale(advance(currentFrequency, startDate, parseInt(+d.offset))); 
		})
		.y(function (d,i) {return tsyrange(+d.val);})
		.interpolate("linear");

	tsgraph = svgContainer.append("path").attr("d", tsLineFunction(data)).attr("class","retrtspath")
		.attr("transform", "translate(" + xloc + "," + yloc + ")");
	//tsgraph = svgContainer.append("g").attr("clip-path","url(#clip)").append("path").attr("d", tsLineFunction(tsData.data)).attr("class","retrtspath")
	//.attr("transform", "translate(" + xloc + "," + yloc + ")");

	tsxtickgroup = svgContainer.selectAll("line.x").attr("transform", "translate(" + xloc + "," + yloc + ")")
	  .data(tsaltxrange.ticks(naltxticks))
	  .enter().append("line")
	  .attr("class", "retrtsxaxisgrid")
	  .attr("x1", function (d){
		  return (xloc + tsaltxrange(+d));
		  })
	  .attr("x2", function (d){
		  return (xloc + tsaltxrange(+d));
		  })
	  .attr("y1", (yloc + height))
	  .attr("y2", (yloc + height + 7))
	  .style("stroke", "#ccc");
	
	tsaltxtickgroup = svgContainer.selectAll("line.altx").attr("transform", "translate(" + xloc + "," + yloc + ")")
	  .data( ((isHf)?tshfxrange.ticks(nxticks):tscale.ticks(nxticks)) )
	  .enter().append("line")
	  .attr("class", "retrtsxaxisgrid")
	  .attr("x1", function (d){
		  return (xloc + tscale(+d));
		  })
	  .attr("x2", function (d){
		  return (xloc + tscale(+d));
		  })
	  .attr("y1", (yloc + height + altxaxisoffset))
	  .attr("y2", (yloc + height + 4 + altxaxisoffset))
	  .style("stroke", "#ccc");
	
	tsytickgroup = svgContainer.selectAll("line.y").attr("transform", "translate(" + xloc + "," + yloc + ")")
	  .data(tsyrange.ticks(nyticks))
	  .enter().append("line")
	  .attr("class", "retrtsyaxisgrid")
	  .attr("x1", function (d){
		  return (xloc);
		  })
	  .attr("x2", function (d){
		  return (xloc - 4);
		  })
	  .attr("y1", function(d) {return (yloc + tsyrange(+d));})
	  .attr("y2", function(d) {return (yloc + tsyrange(+d));})
	  .style("stroke", "#ccc");
	
	tsrow1labelgroup = svgContainer.selectAll("text.altxrow1").attr("transform", "translate(" + xloc + "," + yloc + ")")
	  .data(tscale.ticks(nxticks))
	  .enter().append("text")
	  .attr("class", "retrtsxaxistext")
	  .attr("x", function (d){ return (xloc + tscale(+d)); })
	  .attr("y", function (d){ return (yloc + height + altxaxisoffset + 22); })
	  .text(function(d) {return getRow1TickFormat(currentFrequency,d);})
	  .attr("text-anchor", "middle");
	
	tsrow2labelgroup = svgContainer.selectAll("text.altxrow2").attr("transform", "translate(" + xloc + "," + yloc + ")")
	  .data(tscale.ticks(nxticks))
	  .enter().append("text")
	  .attr("class", "retrtsxaxistext")
	  .attr("x", function (d){ return (xloc + tscale(+d)); })
	  .attr("y", function (d){ return (yloc + height + altxaxisoffset + 30); })
	  .text(function(d) {return getRow2TickFormat(currentFrequency,d);})
	  .attr("text-anchor", "middle");
	
	frequencyTitle = svgContainer.append("text")
	  .attr("class", "retrtsxaxistext")
	  .attr("x", (xloc + width/2))
	  .attr("y", (yloc + height + altxaxisoffset - 5))
	  .text("Data Frequency: " + currentFrequency)
	  .attr("text-anchor", "middle");
	
	tsxaxisgroup = svgContainer.append("g");//.attr("class", "x axis");
	tsxaxisgroup.call(tsxaxis).attr('class','retrtsxaxis')
	.attr("transform", "translate(" + xloc + "," + (yloc + height + altxaxisoffset) + ")")
	.selectAll('text').attr('class','retrtsxaxistext');

	tsaltxaxisgroup = svgContainer.append("g");//.attr("class", "x axis");
	tsaltxaxisgroup.call(tsaltxaxis).attr('class','retrtsaltxaxis')
	.attr("transform", "translate(" + xloc + "," + (yloc + height) + ")")
	.selectAll('text').attr('class','retrtsaltxaxistext');
	
	tsyaxisgroup = svgContainer.append("g").attr("transform", "translate(" + xloc + "," + yloc + ")");//.attr("class", "y axis");;
	tsyaxisgroup.call(tsyaxis).attr('class','retrtsyaxis')
		.selectAll('text').attr('class','retrtsyaxistext')
		.append("text").attr("transform", "rotate(-90)").attr("y", 6).attr("dy", ".71em").style("text-anchor", "end");
	
	tspoints = svgContainer.selectAll(".point")
    	.data(data)
    	.enter().append("svg:circle").attr("transform", "translate(" + xloc + "," + yloc + ")")
    	.attr('class', function(d,i){
    		if (typeof selectedChartIndex != 'undefined' && selectedChartIndex >= 0) {
    			var dimensions = parseInt(document.getElementById('dimensionsID').value);
    			if (i >= selectedChartIndex && i < (selectedChartIndex + dimensions))
    				return 'retrtspointsel';
    			else
        			return 'retrtspoint';
    			
    		}
    		return 'retrtspoint';
    	})
    	.attr("cx", function (d,i) {
    		if (currentFrequency == 'MICROS' || currentFrequency == 'NANOS') {
				return tshfxrange(parseInt(+d.offset));
			} else
				return tscale(advance(currentFrequency, startDate, parseInt(+d.offset)));
		})
    	.attr("cy", function(d, i) { return tsyrange(+d.val); })
    	.attr("r", function(d, i) { return 2; })
    	.on("click", function(d,i) {
    		if (typeof matchJSON != 'undefined' && typeof matchJSON.pattern != 'undefined') {
    			var dimensions = parseInt(document.getElementById('dimensionsID').value);
    			if ( ((i + dimensions - 1) <=  (matchJSON.tsResults.data.length - 1) ) 
        				&& (document.getElementById('matchPatternNameID').selectedIndex == -1) ) {
    				matchJSON.pattern = [];
    				selectedChartIndex = i;
    				for (var j = 0 ; j < matchJSON.tsResults.data.length; j++) {
    					if (j >= selectedChartIndex && j < (selectedChartIndex + dimensions))
    						matchJSON.pattern[matchJSON.pattern.length] = +matchJSON.tsResults.data[j].val;
    				}
    				//setPatternGraph(matchJSON.pattern);
    				//window["refreshChartViewFunc"]();
    				setChartView(matchJSON.tsResults.data, matchJSON.tsResults.startts);
    			}
    		}
    	});
}


function removeMatchGraphs() {
	for (var i=0; i< mggraph.length; i++) {
		if (typeof mggraph[i] != 'undefined')
			mggraph[i].remove();
		if (typeof mgpoints[i] != 'undefined')
			mgpoints[i].remove();
		if (typeof mgxaxisgroup[i] != 'undefined')
			mgxaxisgroup[i].remove();
		if (typeof mgyaxisgroup[i] != 'undefined')
			mgyaxisgroup[i].remove();
		if (typeof mgy1axisgroup[i] != 'undefined')
			mgy1axisgroup[i].remove();
		if (typeof mggraphtitle[i] != 'undefined')
			mggraphtitle[i].remove();
		if (typeof mggraphstart[i] != 'undefined')
			mggraphstart[i].remove();
	}
}

function setMatchGraph(pattern, match, width, height, xloc, yloc, index) {
	
	var x = d3.scale.linear().range([0, width]);
	var y = d3.scale.linear().range([height, 0]);
	var y1 = d3.scale.linear().range([height, 0]);
	var xAxis = d3.svg.axis().scale(x).orient("bottom").ticks(4);
	var yAxis = d3.svg.axis().scale(y).orient("left").ticks(3);
	var y1Axis = d3.svg.axis().scale(y).orient("right").ticks(3);
	//var matchStart = match.matchStart;
	//x.domain(d3.extent(match.results, function(d,i) { return (i + +match.matchStart); }));
	x.domain(d3.extent(match.results, function(d,i) { return i; }));
	y.domain(d3.extent(match.results, function(d,i) { return +d; }));
	y1.domain(d3.extent(pattern, function(d,i) { return +d; }));
	
	var tsLineFunction = d3.svg.line()
		.x(function (d,i) {return x(i);})
		.y(function (d,i) {return y(+d);})
		.interpolate("linear");
	
	//var tsLineFunction1 = d3.svg.line()
	//.x(function (d,i) {return x(i);})
	//.y(function (d,i) {return y1(+d);})
	//.interpolate("linear");
	
	mgpoints[index] = svgContainer.selectAll(".point")
	.data(pattern)
	.enter().append("svg:circle").attr("transform", "translate(" + xloc + "," + yloc + ")").attr('class','retrmtchpoint')
	.attr("cx", function(d, i) { return x(i); })
	.attr("cy", function(d, i) { return y1(+d); })
	.attr("r", function(d, i) { return 2; });
	
	mggraph[index] = svgContainer.append("path").attr("d", tsLineFunction(match.results)).attr('class','retrmtchpath')
		.attr("transform", "translate(" + xloc + "," + yloc + ")");
	
	//mggraph1[index] = svgContainer.append("path").attr("d", tsLineFunction1(pattern)).attr('class','retrmtchpath1')
	//.attr("transform", "translate(" + xloc + "," + yloc + ")");
	
	mgxaxisgroup[index] = svgContainer.append("g").attr("class", "x axis");
	mgxaxisgroup[index].call(xAxis).attr('class','retrmtchxaxis')
		.attr("transform", "translate(" + xloc + "," + (yloc + height) + ")")
		.selectAll('text').attr('class','retrmtchxaxistext');
	
	mgyaxisgroup[index] = svgContainer.append("g").attr("transform", "translate(" + xloc + "," + yloc + ")").attr("class", "y axis");
	mgyaxisgroup[index].call(yAxis).attr('class','retrmtchyaxis')
	.selectAll('text').attr('class','retrmtchyaxistext')
		.append("text").attr("transform", "rotate(-90)").attr("y", 6).attr("dy", ".71em").style("text-anchor", "end");
	
	mgy1axisgroup[index] = svgContainer.append("g").attr("transform", "translate(" + (xloc + width) + "," + yloc + ")").attr("class", "y axis");
	mgy1axisgroup[index].call(y1Axis).attr('class','retrmtchy1axis')
	.selectAll('text').attr('class','retrmtchy1axistext')
		.append("text").attr("transform", "rotate(-90)").attr("y", 6).attr("dy", ".71em").style("text-anchor", "end");
	
	mggraphtitle[index] = svgContainer.append("text")
    .attr("x", (xloc))             
    .attr("y",(yloc-10)).attr('class','retrmtchtitle')
    .text("Node" + match.matchLogicalNumber + ": " + match.matchParameter);
	
	mggraphstart[index] = svgContainer.append("text")
    .attr("x", (xloc))             
    .attr("y",(yloc + height + 25)).attr('class','retrmtchtitle')
    .text("i = " + match.matchStart + ", r = " + match.actualDistance);
	
}

function setPatternGraph(pattern) {
	var width = patternGraph_width;
	var height = patternGraph_height;
	var xloc = patternGraph_xloc;
	var yloc = patternGraph_yloc;

	if (!pattern || pattern.length == 0)
		return;
	
	if (typeof pgpoints != 'undefined')
		pgpoints.remove();
	if (typeof pggraph != 'undefined')
		pggraph.remove();
	if (typeof pgxaxisgroup != 'undefined')
		pgxaxisgroup.remove();
	if (typeof pgyaxisgrouplabeltext != 'undefined')
		pgyaxisgrouplabeltext.remove();
	if (typeof pgyaxisgrouplabel != 'undefined')
		pgyaxisgrouplabel.remove();
	if (typeof pgyxisgroup != 'undefined')
		pgyxisgroup.remove();
		
	var x = d3.scale.linear().range([0, width]);
	var y = d3.scale.linear().range([height, 0]);
	var xAxis = d3.svg.axis().scale(x).orient("bottom").ticks(4);
	var yAxis = d3.svg.axis().scale(y).orient("left").ticks(3);
	
	x.domain(d3.extent(pattern, function(d,i) { return i; }));
	y.domain(d3.extent(pattern, function(d,i) { return +d; }));
	var tsLineFunction = d3.svg.line()
		.x(function (d,i) {return x(i);})
		.y(function (d,i) {return y(+d);})
		.interpolate("linear");
	
	pgpoints = svgContainer.selectAll(".point")
	.data(pattern)
	.enter().append("svg:circle").attr("transform", "translate(" + xloc + "," + yloc + ")").attr('class','retrptrnpoint')
	//.attr("stroke", "red")
	//.attr("fill", function(d, i) { return "none"; })
	.attr("cx", function(d, i) { return x(i); })
	.attr("cy", function(d, i) { return y(+d); })
	.attr("r", function(d, i) { return 2; });
	
	pggraph = svgContainer.append("path").attr("d", tsLineFunction(pattern)).attr('class','retrptrnpath')
	.attr("transform", "translate(" + xloc + "," + yloc + ")");

	pgxaxisgroup = svgContainer.append("g").attr("class", "x axis");
	pgxaxisgroup.call(xAxis).attr('class','retrptrnxaxis')
		.attr("transform", "translate(" + xloc + "," + (yloc + height) + ")")
		.selectAll('text').attr('class','retrptrnxaxistext');
	
	pgyaxisgroup = svgContainer.append("g").attr("transform", "translate(" + xloc + "," + yloc + ")").attr("class", "y axis");
	pgyaxisgrouplabel = pgyaxisgroup.call(yAxis).attr('class','retrptrnyaxis');
	pgyaxisgrouplabeltext =	pgyaxisgrouplabel.selectAll('text').attr('class','retrptrnyaxistext')
	.append("text").attr("transform", "rotate(-90)").attr("y", 6)
		.attr("dy", ".71em").style("text-anchor", "end");
}


function configurationSelectionNotification(result, textStatus, jQxhr) {
	var ajaxJson = JSON.parse(result);
	activeLn = ajaxJson.match.ln;
	nodeConfig = ajaxJson.match.nodeConfig;
	capturedPatterns = ajaxJson.match.capturedPatterns;
	populateSelect('tsParameterID',getConfigParameterNames(nodeConfig));
	//searchSelections = [];

	if (typeof nodeConfig != 'undefined' && typeof nodeConfig[0] != 'undefined' && typeof nodeConfig[0].datatype != 'undefined') {
		//alert("fetching pattern");
		document.getElementById('tsStartDateID').value = nodeConfig[0].start;
		document.getElementById('tsEndDateID').value= nodeConfig[0].end;
		populateSelect('matchPatternNameID', getCapturedPatternNames(nodeConfig[0].datatype));
		populateSelect('tsWindowSizeID',windowSizes);
		populateSelect('dimensionsID',dimensionSizes);
		
		if (nodeConfig[0].isPatternIndexed == "true")
			document.getElementById('dimensionsID').value = nodeConfig[0].patternIndexSize;
		
		tsViewStartts = getConfigValue(document.getElementById('tsParameterID').value, nodeConfig, 'startts');
		tsViewEndts = getConfigValue(document.getElementById('tsParameterID').value, nodeConfig, 'endts');		
		document.getElementById('datatypeID').innerText = getConfigValue(document.getElementById('tsParameterID').value, nodeConfig, 'datatype');
		
		currentFrequency = nodeConfig[0].frequency;
		if (currentFrequency == 'MICROS' || currentFrequency == 'NANOS') {
			document.getElementById('tsPanelStartHfOffsetID').style.display = 'inline';
			document.getElementById('tsPanelEndHfOffsetID').style.display = 'inline';
			document.getElementById('tsPanelStartHfOffsetID').value = 0;
			document.getElementById('tsPanelEndHfOffsetID').value = ((currentFrequency == 'MICROS')?999:999999);
		} else {
			document.getElementById('tsPanelStartHfOffsetID').style.display = 'none';
			document.getElementById('tsPanelEndHfOffsetID').style.display = 'none';
			document.getElementById('tsPanelStartHfOffsetID').value = -1;
			document.getElementById('tsPanelEndHfOffsetID').value = -1;
		}
		tsViewStartHfOffset = -1;
		tsViewEndHfOffset = -1;
		selectedChartIndex = -1;
		fetchPattern();
		window["refreshChartViewFunc"]();
	}
	notificationPanel.text("Configuration action completed for action " + ajaxJson.match.action); 
	messagePanel.text("OK");
	setClusterStatusASync();
}

function getCapturedPatternNames(datatype) {
	if (datatype == "int" && typeof capturedPatterns.intPatterns != 'undefined') {
		return capturedPatterns.intPatterns;
	} else if (datatype == "double" && typeof capturedPatterns.doublePatterns != 'undefined') {
		return capturedPatterns.doublePatterns;
	} else if (datatype == "float" && typeof capturedPatterns.floatPatterns != 'undefined') {
		return capturedPatterns.floatPatterns;
	} else if (datatype == "long" && typeof capturedPatterns.longPatterns != 'undefined') {
		return capturedPatterns.longPatterns;
	} else if (datatype == "boolean" && typeof capturedPatterns.booleanPatterns != 'undefined') {
		return capturedPatterns.booleanPatterns;
	}
	return [];
}


function createMatchesNodeSelectionFunction(idx) {
	var i = idx;
    return function () {
    	
    	//no configuration required for replica nodes 
    	if (clusterStatus && (clusterStatus[i].replicaOf != clusterStatus[i].logicalNumber))
    		return;
    	
    	for (var n = 0; n < nodes.length; n++) {
    		if ( n == i) {
    			nodes[n].attr("selected","true");
    			nodes[n].attr("rx", "" + selectedEllipse);
      			nodes[n].attr("ry", "" + selectedEllipse);
    		} else {
    			nodes[n].attr("selected","false");
    			nodes[n].attr("rx", "" + unselectedEllipse);
      			nodes[n].attr("ry", "" + unselectedEllipse);
    		}
    	}
  			
		var webJson = new Object();
		var matchJSON = new Object();
		matchJSON.tsResults = new Object();
		matchJSON.tsResults.ln = "" + clusterStatus[i].logicalNumber;
		matchJSON.tsResults.data = [];
		matchJSON.ln = "" + clusterStatus[i].logicalNumber;
		matchJSON.cluster = clusterStatus[i].cluster;
		matchJSON.action = "DR_CONFIGURE";
		webJson.match = matchJSON;
		//alert(JSON.stringify(webJson));
		doAjax("/ajaxHandler", JSON.stringify(webJson), configurationSelectionNotification, reportError);

	};
}



