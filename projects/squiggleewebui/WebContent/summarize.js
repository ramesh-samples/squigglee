// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
var sketchJSON = new Object();
var searchSelections = [];
var spRow1, spLogicalNumber,spMin, spMax, spFirst, spLast, spCount, spUpdateStatisticsButton, spUpdateStatisticsButtonOn;
var spRow2, spParameter, spFrequentItems, spRow3, spPointQuery, spPointQueryResult, spPointQueryButton, spPointQueryButtonOn, spInverseQueryButton, spInverseQueryButtonOn;
var spRow4, spRangeResults, spRangeQueryResult, spInverseQueryResult, spUpdateHistogramButton, spUpdateHistogramButtonOn, spRow5;
var samplingRow1, sampleSize, samplingRow2, tsSamplingParameter, samplingRow3, samplingRow4, samplingStartDate, samplingEndDate;
var spBar=[], graphTitle=[], spHistXaxisGroup=[], spHistXaxis=[], spHistXaxisLabel=[], spHistYaxisGroup=[], spHistYaxis=[], spHistYaxisLabel=[];
var captureDimensionality;
var tsParameter;
var tsStartDate;
var tsEndDate;
var capturePatternStartDate;
var capturePatternButton;

//capture panel
var samplingLogicalNumber, samplingDataType, dataType, sampleBins;
var patternDimensionality;
var patternParameterName;
var patternStartDate;
var patternEndDate;
var addTsToSearchButton, addTsToSearchButtonOn;
var removeTsToSearchButton, removeTsToSearchButtonOn;
var updateSet1Button, updateSet1ButtonOn, updateSet2Button, updateSet2ButtonOn, updateSet3Button, updateSet3ButtonOn;
var summarizeLayout, searchTable;
var cb = [];
var colCounter;
var colInterval;
var rowInterval;
var headerStart;

function setSummarizeView() {
	setRectangleView('Synopses');
	for (var i=0; i< nodes.length; i++)
		nodes[i].on("click",createSketchesNodeSelectionFunction(i));

	if (typeof activeLn != 'undefined') {
		for (var i=0; i<clusterStatus.length; i++) {
			if (clusterStatus[i].logicalNumber == activeLn) {
				nodes[i].attr("selected", "true");
	  			nodes[i].attr("rx", "" + selectedEllipse);
	  			nodes[i].attr("ry", "" + selectedEllipse);
	  			//nodes[i].attr("stroke-opacity","1");
			}
		}
	}
	
	if (pageLoad) {
		setControls();	
		addSelectionTable();
	}
	
	if(pageLoad)
		pageLoad = false;
}

function setControls() {
	summarizeLayout = svgContainer.append("foreignObject").attr("width", 390).attr("height", 450).attr("y",75).attr("x",(60))
	.html("<table width=\"390px\" id=\"summarizeLayoutID\" ></table>");
	var layout = document.getElementById('summarizeLayoutID');
	layout.setAttribute('class','summlayout');
	
	var row0 = layout.insertRow(0);
	row0.id = 'summarizeLayoutRowID0';
	var row1 = layout.insertRow(1);
    row1.id = 'summarizeLayoutRowID1';
    var row2 = layout.insertRow(2);
    row2.id = 'summarizeLayoutRowID2';
    var row3 = layout.insertRow(3);
    row3.id = 'summarizeLayoutRowID3';
    var row4 = layout.insertRow(4);
    row4.id = 'summarizeLayoutRowID4';
    var row5 = layout.insertRow(5);
    row5.id = 'summarizeLayoutRowID5';
    var row6 = layout.insertRow(6);
    row6.id = 'summarizeLayoutRowID6';
    var row7 = layout.insertRow(7);
    row7.id = 'summarizeLayoutRowID7';
    var row8 = layout.insertRow(8);
    row8.id = 'summarizeLayoutRowID8';
    var row9 = layout.insertRow(9);
    row9.id = 'summarizeLayoutRowID9';
    var row10 = layout.insertRow(10);
    row10.id = 'summarizeLayoutRowID10';
    var row11 = layout.insertRow(11);
    row11.id = 'summarizeLayoutRowID11';
    var row12 = layout.insertRow(12);
    row12.id = 'summarizeLayoutRowID12';
    var row13 = layout.insertRow(13);
    row13.id = 'summarizeLayoutRowID13';
    
    var titleLabel1 = document.createElement("span");
    titleLabel1.innerHTML = "<p>Sketching</p>";
    titleLabel1.setAttribute('class','summtabletitle');
	
    var tsIdLabel = document.createElement("span");
    tsIdLabel.innerHTML = "Time Series ID";
    tsIdLabel.setAttribute('class','controllabel');
    
    var tsIdSelect = document.createElement("select");
    tsIdSelect.id = "tsParameterID";
    
    var dataTypeLabel = document.createElement("span");
    dataTypeLabel.innerHTML = "Data Type:";
    dataTypeLabel.setAttribute('class','controllabel');
    
    dataType = document.createElement("input");
    dataType.type = "text";
    dataType.size = "8";
    dataType.id = "dataTypeID";
    dataType.setAttribute('class','result');
    
    spUpdateStatisticsButtonOn = function() {
		if (typeof activeLn != 'undefined') { 
			
			if (getConfigValue(document.getElementById('tsParameterID').value, nodeConfig, 'isSketched') != "true") {
				alert('This time series has not been sketched.');
				return;
			}
				
			var webJson = new Object();
			var summarizeJSON = getPostBackParameters();
			summarizeJSON.action = "SK_UPDATE_STATS";
			webJson.summarize = summarizeJSON;
			//alert(JSON.stringify(webJson));
			doAjax("/ajaxHandler", JSON.stringify(webJson), updateStatistics, reportError);	// nothing to do, later refresh the list of captured patterns 
		}
	};
    spUpdateStatisticsButton = document.createElement("button");
    spUpdateStatisticsButton.innerText = "Stats";
    spUpdateStatisticsButton.id = "spUpdateStatisticsButtonID";
    spUpdateStatisticsButton.readonly = true;
    spUpdateStatisticsButton.setAttribute('class','summButtonEnabled');
    spUpdateStatisticsButton.onclick = spUpdateStatisticsButtonOn;
    
    spPointQueryButtonOn = function() {
		if (typeof activeLn != 'undefined') { 
			
			if (getConfigValue(document.getElementById('tsParameterID').value, nodeConfig, 'isSketched') != "true") {
				alert('This time series has not been sketched.');
				return;
			}
			
			var webJson = new Object();
			var summarizeJSON = getPostBackParameters();
			summarizeJSON.action = "SK_POINT_QUERY";
			webJson.summarize = summarizeJSON;
			//alert(JSON.stringify(webJson));
			doAjax("/ajaxHandler", JSON.stringify(webJson), pointQuery, reportError);	// nothing to do, later refresh the list of captured patterns 
		}
	};
	spPointQueryButton = document.createElement("button");
	spPointQueryButton.innerText = "Point Query";
	spPointQueryButton.id = "spPointQueryButtonID";
	spPointQueryButton.readonly = true;
	spPointQueryButton.setAttribute('class','summButtonEnabled');
	spPointQueryButton.onclick = spPointQueryButtonOn;
    
	spRangeQueryButtonOn = function() {
		if (typeof activeLn != 'undefined') { 
			
			if (getConfigValue(document.getElementById('tsParameterID').value, nodeConfig, 'isSketched') != "true") {
				alert('This time series has not been sketched.');
				return;
			}
			
			var webJson = new Object();
			var summarizeJSON = getPostBackParameters();
			summarizeJSON.action = "SK_RANGE_QUERY";
			webJson.summarize = summarizeJSON;
			//alert(JSON.stringify(webJson));
			doAjax("/ajaxHandler", JSON.stringify(webJson), rangeQuery, reportError);	// nothing to do, later refresh the list of captured patterns 
		}
	};
	spRangeQueryButton = document.createElement("button");
	spRangeQueryButton.innerText = "Range Query";
	spRangeQueryButton.id = "spRangeQueryButtonID";
	spRangeQueryButton.readonly = true;
	spRangeQueryButton.setAttribute('class','summButtonEnabled');
	spRangeQueryButton.onclick = spRangeQueryButtonOn;
	
	spInverseQueryButtonOn = function() {
		if (typeof activeLn != 'undefined') { 
			
			if (getConfigValue(document.getElementById('tsParameterID').value, nodeConfig, 'isSketched') != "true") {
				alert('This time series has not been sketched.');
				return;
			}
			
			var webJson = new Object();
			var summarizeJSON = getPostBackParameters();
			summarizeJSON.action = "SK_INVERSE_QUERY";
			webJson.summarize = summarizeJSON;
			//alert(JSON.stringify(webJson));
			doAjax("/ajaxHandler", JSON.stringify(webJson), inverseQuery, reportError);	// nothing to do, later refresh the list of captured patterns 
		}
	};
	spInverseQueryButton = document.createElement("button");
	spInverseQueryButton.innerText = "Inverse Query";
	spInverseQueryButton.id = "spInverseQueryButtonID";
	spInverseQueryButton.readonly = true;
	spInverseQueryButton.setAttribute('class','summButtonEnabled');
	spInverseQueryButton.onclick = spInverseQueryButtonOn;
	
	spUpdateHistogramButtonOn = function() {
		if (typeof activeLn != 'undefined') { 
			
			if (getConfigValue(document.getElementById('tsParameterID').value, nodeConfig, 'isSketched') != "true") {
				alert('This time series has not been sketched.');
				return;
			}
			
			var webJson = new Object();
			var summarizeJSON = getPostBackParameters();
			summarizeJSON.action = "SK_UPDATE_HISTOGRAM";
			webJson.summarize = summarizeJSON;
			//alert(JSON.stringify(webJson));
			doAjax("/ajaxHandler", JSON.stringify(webJson), updateHistogram, reportError);	// nothing to do, later refresh the list of captured patterns 
		}
	};
	spUpdateHistogramButton = document.createElement("button");
	spUpdateHistogramButton.innerText = "Histogram";
	spUpdateHistogramButton.id = "spUpdateHistogramButtonID";
	spUpdateHistogramButton.readonly = true;
	spUpdateHistogramButton.setAttribute('class','summButtonEnabled');
	spUpdateHistogramButton.onclick = spUpdateHistogramButtonOn;
	
    var statsMinLabel = document.createElement("span");
    statsMinLabel.innerHTML = "Min:";
    statsMinLabel.setAttribute('class','controllabel');
    
    spMin = document.createElement("input");
    spMin.type = "text";
    spMin.setAttribute('class','result');
    spMin.size = "8";
    
    var statsMaxLabel = document.createElement("span");
    statsMaxLabel.innerHTML = "Max:";
    statsMaxLabel.setAttribute('class','controllabel');
    
    spMax = document.createElement("input");
    spMax.type = "text";
    spMax.setAttribute('class','result');
    spMax.size = "8";
    
    var statsFirstLabel = document.createElement("span");
    statsFirstLabel.innerHTML = "First:";
    statsFirstLabel.setAttribute('class','controllabel');
    
    spFirst = document.createElement("input");
    spFirst.type = "text";
    spFirst.setAttribute('class','result');
    spFirst.size = "8";
    
    var statsLastLabel = document.createElement("span");
    statsLastLabel.innerHTML = "Last:";
    statsLastLabel.setAttribute('class','controllabel');
    
    spLast = document.createElement("input");
    spLast.type = "text";
    spLast.setAttribute('class','result');
    spLast.size = "8";
    
    var statsCountLabel = document.createElement("span");
    statsCountLabel.innerHTML = "Count:";
    statsCountLabel.setAttribute('class','controllabel');
    
    spCount = document.createElement("input");
    spCount.type = "text";
    spCount.setAttribute('class','result');
    spCount.size = "8";
    
    var statsHHLabel = document.createElement("span");
    statsHHLabel.innerHTML = "Heavy Hitters:";
    statsHHLabel.setAttribute('class','controllabel');
    
    spFrequentItems = document.createElement("input");
    spFrequentItems.type = "text";
    spFrequentItems.setAttribute('class','result');
    spFrequentItems.size = "8";
    
    var pointValueLabel = document.createElement("span");
    pointValueLabel.innerHTML = "Value:";
    pointValueLabel.setAttribute('class','controllabel');
    pointValueLabel.setAttribute('class','controllabel');
    var pointResultLabel = document.createElement("span");
    pointResultLabel.innerHTML = "Point Result:";
    pointResultLabel.setAttribute('class','controllabel');
    
    spPointQueryResult = document.createElement("input");
    spPointQueryResult.type = "text";
    spPointQueryResult.setAttribute('class','result');
    spPointQueryResult.size = "8";
    
    var rangeValueLabel = document.createElement("span");
    rangeValueLabel.innerHTML = "Value Range:";
    rangeValueLabel.setAttribute('class','controllabel');
    var rangeResultLabel = document.createElement("span");
    rangeResultLabel.innerHTML = "Range Result:";
    rangeResultLabel.setAttribute('class','controllabel');
    
    spRangeQueryResult = document.createElement("input");
    spRangeQueryResult.type = "text";
    spRangeQueryResult.setAttribute('class','result');
    spRangeQueryResult.size = "8";
    
    var inverseValueLabel = document.createElement("span");
    inverseValueLabel.innerHTML = "Quantile (%):";
    inverseValueLabel.setAttribute('class','controllabel');
    var inverseResultLabel = document.createElement("span");
    inverseResultLabel.innerHTML = "Inverse Result:";
    inverseResultLabel.setAttribute('class','controllabel');
    
    spInverseQueryResult = document.createElement("input");
    spInverseQueryResult.type = "text";
    spInverseQueryResult.setAttribute('class','result');
    spInverseQueryResult.size = "8";
    
    var pointQueryBox = document.createElement("input");
    pointQueryBox.type = "text";
    pointQueryBox.size = 15;
    pointQueryBox.id = "pointQueryID";
    
    var range1QueryBox = document.createElement("input");
    range1QueryBox.type = "text";
    range1QueryBox.size = 5;
    range1QueryBox.id = "range1ID";
    
    var range2QueryBox = document.createElement("input");
    range2QueryBox.type = "text";
    range2QueryBox.size = 5;
    range2QueryBox.id = "range2ID";
    
    var inverseQueryBox = document.createElement("input");
    inverseQueryBox.type = "text";
    inverseQueryBox.size = 15;
    inverseQueryBox.value = "50";
    inverseQueryBox.id = "inverseQueryID";
    
    var sketchBinsLabel = document.createElement("span");
    sketchBinsLabel.innerHTML = "Bins:";
    sketchBinsLabel.setAttribute('class','controllabel');
    
    var sketchBinsBox = document.createElement("input");
    sketchBinsBox.type = "text";
    sketchBinsBox.size = 15;
    sketchBinsBox.value = "10";
    sketchBinsBox.id = "spBinsID";
    
    var td0 = document.createElement('td'); 
    td0.colSpan = "8";
    td0.appendChild(titleLabel1);
    row0.appendChild(td0);
    
    var td1 = document.createElement('td'); 
    td1.colSpan = "1";
    td1.appendChild(tsIdLabel);
    row1.appendChild(td1);
    
    var td2 = document.createElement('td'); 
    td2.colSpan = "1";
    td2.appendChild(tsIdSelect);
    row1.appendChild(td2);
    
    var td9 = document.createElement('td'); 
    td9.colSpan = "1";
    //td9.appendChild(spUpdateStatisticsButton);
    td9.appendChild(getBlankColumn(1));
    row1.appendChild(td9);
    
    var td1a = document.createElement('td'); 
    td1a.colSpan = "1";
    td1a.appendChild(dataTypeLabel);
    row1.appendChild(td1a);
    
    var td2a = document.createElement('td'); 
    td2a.colSpan = "1";
    td2a.appendChild(dataType);
    row1.appendChild(td2a);
    
    row1.appendChild(getBlankColumn(3));
    
    //row2.appendChild(getBlankColumn(1));
    
    var td3 = document.createElement('td'); 
    td3.colSpan = "1";
    td3.appendChild(statsMinLabel);
    td3.appendChild(getBlankSpan());td3.appendChild(getBlankSpan());td3.appendChild(getBlankSpan());
    td3.appendChild(spMin);
    row2.appendChild(td3);
    
    var td4 = document.createElement('td'); 
    td4.colSpan = "1";
    td4.appendChild(statsMaxLabel);
    td4.appendChild(getBlankSpan());td4.appendChild(getBlankSpan());td4.appendChild(getBlankSpan());
    td4.appendChild(spMax);
    row2.appendChild(td4);
    
    //var td4b = document.createElement('td'); 
    //td4b.colSpan = "1";
    //td4b.appendChild(statsMaxLabel);
    //row2.appendChild(td4b);
    
    var td5 = document.createElement('td'); 
    td5.colSpan = "1";
    td5.rowSpan = "2";
    td5.appendChild(spUpdateStatisticsButton);
    row2.appendChild(td5);
    
    var td4a = document.createElement('td'); 
    td4a.colSpan = "1";
    td4a.appendChild(statsCountLabel);
    
    row2.appendChild(td4a);
    var td5a = document.createElement('td'); 
    td5a.colSpan = "1";
    td5a.appendChild(spCount);
    row2.appendChild(td5a);
    
    row2.appendChild(getBlankColumn(3));
    
    //row3.appendChild(getBlankColumn(1));
    
    var td6 = document.createElement('td'); 
    td6.colSpan = "1";
    td6.appendChild(statsFirstLabel);
    td6.appendChild(getBlankSpan());td6.appendChild(getBlankSpan());
    td6.appendChild(spFirst);
    row3.appendChild(td6);
    
    //var td7 = document.createElement('td'); 
    //td7.colSpan = "1";
    //td7.appendChild(spFirst);
    //row3.appendChild(td7);
    
    var td8 = document.createElement('td'); 
    td8.colSpan = "1";
    td8.appendChild(statsLastLabel);
    td8.appendChild(getBlankSpan());td8.appendChild(getBlankSpan());td8.appendChild(getBlankSpan());
    td8.appendChild(spLast);
    row3.appendChild(td8);
    
    //var td9a = document.createElement('td'); 
    //td9a.colSpan = "1";
    //td9a.appendChild(spLast);
    //row3.appendChild(td9a);
    
    //row3.appendChild(getBlankColumn(1));
    
    var td10 = document.createElement('td'); 
    td10.colSpan = "1";
    td10.appendChild(statsHHLabel);
    row3.appendChild(td10);
    
    var td11 = document.createElement('td'); 
    td11.colSpan = "1";
    td11.appendChild(spFrequentItems);
    row3.appendChild(td11);
    
    row3.appendChild(getBlankColumn(3));

    var td10a = document.createElement('td'); 
    td10a.colSpan = "1";
    td10a.appendChild(pointValueLabel);
    row4.appendChild(td10a);
    
    var td11a = document.createElement('td'); 
    td11a.colSpan = "1";
    td11a.appendChild(pointQueryBox);
    row4.appendChild(td11a);
    
    var td14 = document.createElement('td'); 
    td14.colSpan = "1";
    td14.appendChild(spPointQueryButton);
    row4.appendChild(td14);
    
    var td12 = document.createElement('td'); 
    td12.colSpan = "1";
    td12.appendChild(pointResultLabel);
    row4.appendChild(td12);
    
    var td13 = document.createElement('td'); 
    td13.colSpan = "1";
    td13.appendChild(spPointQueryResult);
    row4.appendChild(td13);
    
    row4.appendChild(getBlankColumn(3));

    var td15 = document.createElement('td'); 
    td15.colSpan = "1";
    td15.appendChild(rangeValueLabel);
    row5.appendChild(td15);
    
    var td16 = document.createElement('td'); 
    td16.colSpan = "1";
    td16.appendChild(range1QueryBox);
    td16.appendChild(getBlankSpan());td16.appendChild(getBlankSpan());
    td16.appendChild(range2QueryBox);
    row5.appendChild(td16);
    
    //var td17 = document.createElement('td'); 
    //td17.colSpan = "1";
    //td17.appendChild(range2QueryBox);
    //row7.appendChild(td17);
    var td20 = document.createElement('td'); 
    td20.colSpan = "1";
    td20.appendChild(spRangeQueryButton);
    row5.appendChild(td20);
    
    var td18 = document.createElement('td'); 
    td18.colSpan = "1";
    td18.appendChild(rangeResultLabel);
    row5.appendChild(td18);
    
    var td19 = document.createElement('td'); 
    td19.colSpan = "1";
    td19.appendChild(spRangeQueryResult);
    row5.appendChild(td19);
    
    row5.appendChild(getBlankColumn(3));

    //row8.appendChild(getBlankColumn(2));
    
    var td21 = document.createElement('td'); 
    td21.colSpan = "1";
    td21.appendChild(inverseValueLabel);
    row6.appendChild(td21);
    
    var td22 = document.createElement('td'); 
    td22.colSpan = "1";
    td22.appendChild(inverseQueryBox);
    row6.appendChild(td22);
    
    var td25 = document.createElement('td'); 
    td25.colSpan = "1";
    td25.appendChild(spInverseQueryButton);
    row6.appendChild(td25);
    
    var td23 = document.createElement('td'); 
    td23.colSpan = "1";
    td23.appendChild(inverseResultLabel);
    row6.appendChild(td23);
    
    var td24 = document.createElement('td'); 
    td24.colSpan = "1";
    td24.appendChild(spInverseQueryResult);
    row6.appendChild(td24);
    
    row6.appendChild(getBlankColumn(3));
    
    var td26 = document.createElement('td'); 
    td26.colSpan = "1";
    td26.appendChild(sketchBinsLabel);
    row7.appendChild(td26);
    
    var td27 = document.createElement('td'); 
    td27.colSpan = "1";
    td27.appendChild(sketchBinsBox);
    row7.appendChild(td27);
    
    var td28 = document.createElement('td'); 
    td28.colSpan = "1";
    td28.appendChild(spUpdateHistogramButton);
    row7.appendChild(td28);

    row7.appendChild(getBlankColumn(5));
    
    document.getElementById('tsParameterID').onchange = function() {
    	var selectedParameter = document.getElementById('tsParameterID').value;
    	if (typeof selectedParameter != 'undefined')
    		dataType.value = getConfigValue(selectedParameter, nodeConfig, 'datatype');
    	else
    		dataType.value = "";
    };
    
    // sampling panel
    
    var titleLabel2 = document.createElement("span");
    titleLabel2.innerHTML = "<p>Sampling</p>";
    titleLabel2.setAttribute('class','summtabletitle');
    
    var samplingIdLabel = document.createElement("span");
    samplingIdLabel.innerHTML = "Time Series ID";
    samplingIdLabel.setAttribute('class','controllabel');
    
    var samplingIdSelect = document.createElement("select");
    samplingIdSelect.id = "tsSamplingParameterID";
    
    var samplingDataTypeLabel = document.createElement("span");
    samplingDataTypeLabel.innerHTML = "Data Type:";
    samplingDataTypeLabel.setAttribute('class','controllabel');
    
    samplingDataType = document.createElement("span");
    samplingDataType.id = "samplingDataTypeID";
    
    var samplingStartLabel = document.createElement("span");
    samplingStartLabel.innerHTML = "Start Date:";
    samplingStartLabel.setAttribute('class','controllabel');
    
    var samplingStartBox = document.createElement("input");
    samplingStartBox.type = "text";
    samplingStartBox.size = 25;
    samplingStartBox.id = "samplingStartDateID";
    
    var samplingEndLabel = document.createElement("span");
    samplingEndLabel.innerHTML = "End Date:";
    samplingEndLabel.setAttribute('class','controllabel');
    
    var samplingEndBox = document.createElement("input");
    samplingEndBox.type = "text";
    samplingEndBox.size = 25;
    samplingEndBox.id = "samplingEndDateID";
    
    var samplingSizeLabel = document.createElement("span");
    samplingSizeLabel.innerHTML = "Sample Size:";
    samplingSizeLabel.setAttribute('class','controllabel');
    var samplingBinsLabel = document.createElement("span");
    samplingBinsLabel.innerHTML = "Bins:";
    samplingBinsLabel.setAttribute('class','controllabel');
    
    var samplingSizeBox = document.createElement("input");
    samplingSizeBox.type = "text";
    samplingSizeBox.size = 10;
    samplingSizeBox.value = "1000";
    samplingSizeBox.id = "sampleSizeID";
    
    var samplingBinsBox = document.createElement("input");
    samplingBinsBox.type = "text";
    samplingBinsBox.size = 10;
    samplingBinsBox.value = "10";
    samplingBinsBox.id = "sampleBinsID";
    
    var tds0 = document.createElement('td'); 
    tds0.colSpan = "8";
    tds0.appendChild(titleLabel2);
    row8.appendChild(tds0);
    
    var tds1 = document.createElement('td'); 
    tds1.colSpan = "1";
    tds1.appendChild(samplingIdLabel);
    row9.appendChild(tds1);
    
    var tds2 = document.createElement('td'); 
    tds2.colSpan = "1";
    tds2.appendChild(samplingIdSelect);
    row9.appendChild(tds2);
    
    row9.appendChild(getBlankColumn(1));
    
    var tds3 = document.createElement('td'); 
    tds3.colSpan = "1";
    tds3.appendChild(samplingDataTypeLabel);
    row9.appendChild(tds3);
    
    var tds4 = document.createElement('td'); 
    tds4.colSpan = "1";
    tds4.appendChild(samplingDataType);
    row9.appendChild(tds4);
    
    row9.appendChild(getBlankColumn(3));

    //row10.appendChild(getBlankColumn(1));
    
    var tds5 = document.createElement('td'); 
    tds5.colSpan = "1";
    tds5.appendChild(samplingStartLabel);
    row10.appendChild(tds5);
    
    var tds6 = document.createElement('td'); 
    tds6.colSpan = "2";
    tds6.appendChild(samplingStartBox);
    row10.appendChild(tds6);
    
    var tds7 = document.createElement('td'); 
    tds7.colSpan = "1";
    tds7.appendChild(samplingSizeLabel);
    row10.appendChild(tds7);
    
    var tds8 = document.createElement('td'); 
    tds8.colSpan = "1";
    tds8.appendChild(samplingSizeBox);
    row10.appendChild(tds8);
    
    row10.appendChild(getBlankColumn(2));
    
    //row11.appendChild(getBlankColumn(1));
    
    var tds9 = document.createElement('td'); 
    tds9.colSpan = "1";
    tds9.appendChild(samplingEndLabel);
    row11.appendChild(tds9);
    
    var tds10 = document.createElement('td'); 
    tds10.colSpan = "2";
    tds10.appendChild(samplingEndBox);
    row11.appendChild(tds10);
    
    var tds11 = document.createElement('td'); 
    tds11.colSpan = "1";
    tds11.appendChild(samplingBinsLabel);
    row11.appendChild(tds11);
    
    var tds12 = document.createElement('td'); 
    tds12.colSpan = "1";
    tds12.appendChild(samplingBinsBox);
    row11.appendChild(tds12);
    
    row11.appendChild(getBlankColumn(2));
    
    addTsToSearchButtonOn = function() {addToSearchList();};
	addTsToSearchButton = document.createElement("button");
	addTsToSearchButton.innerText = "Add";
	addTsToSearchButton.id = "AddButtonID";
	addTsToSearchButton.readonly = true;
	addTsToSearchButton.setAttribute('class','summButtonEnabled');
	addTsToSearchButton.onclick = addTsToSearchButtonOn;
    
	removeTsToSearchButtonOn = function() {removeFromSearchList();};
	removeTsToSearchButton = document.createElement("button");
	removeTsToSearchButton.innerText = "Remove";
	removeTsToSearchButton.id = "RemoveButtonID";
	removeTsToSearchButton.readonly = true;
	removeTsToSearchButton.setAttribute('class','summButtonEnabled');
	removeTsToSearchButton.onclick = removeTsToSearchButtonOn;
	
	var tds13 = document.createElement('td'); 
    tds13.colSpan = "8";
    tds13.appendChild(addTsToSearchButton);
    tds13.appendChild(getBlankSpan());
    tds13.appendChild(getBlankSpan());
    tds13.appendChild(getBlankSpan());
    tds13.appendChild(getBlankSpan());
    tds13.appendChild(getBlankSpan());
    tds13.appendChild(removeTsToSearchButton);
    row12.appendChild(tds13);
	
	updateSet1ButtonOn = function() {
		if (typeof activeLn != 'undefined') { 
			var webJson = new Object();
			var summarizeJSON = getPostBackParameters();
			summarizeJSON.action = "SK_UPDATE_SAMPLING_HISTOGRAM";
			summarizeJSON.samplingHistogramRequest = getSampledDataParameters(0);
			webJson.summarize = summarizeJSON;
			//alert(JSON.stringify(webJson));
			if (summarizeJSON.samplingHistogramRequest != null)
				doAjax("/ajaxHandler", JSON.stringify(webJson), updateSet, reportError);	// nothing to do, later refresh the list of captured patterns 
		}
	};
	updateSet1Button = document.createElement("button");
	updateSet1Button.innerText = "Histogram";
	updateSet1Button.id = "UpdateSet1ButtonID";
	updateSet1Button.readonly = true;
	updateSet1Button.setAttribute('class','summButtonEnabledInRow');
	updateSet1Button.onclick = updateSet1ButtonOn;
	
	updateSet2ButtonOn = function() {
		if (typeof activeLn != 'undefined') { 
			var webJson = new Object();
			var summarizeJSON = getPostBackParameters();
			summarizeJSON.action = "SK_UPDATE_SAMPLING_HISTOGRAM";
			summarizeJSON.samplingHistogramRequest = getSampledDataParameters(1);
			webJson.summarize = summarizeJSON;
			//alert(JSON.stringify(webJson));
			if (summarizeJSON.samplingHistogramRequest != null)
				doAjax("/ajaxHandler", JSON.stringify(webJson), updateSet, reportError);	// nothing to do, later refresh the list of captured patterns 
		}
	};
	updateSet2Button = document.createElement("button");
	updateSet2Button.innerText = "Histogram";
	updateSet2Button.id = "UpdateSet2ButtonID";
	updateSet2Button.readonly = true;
	updateSet2Button.setAttribute('class','summButtonEnabledInRow');
	updateSet2Button.onclick = updateSet2ButtonOn;
	
	updateSet3ButtonOn = function() {
		if (typeof activeLn != 'undefined') { 
			var webJson = new Object();
			var summarizeJSON = getPostBackParameters();
			summarizeJSON.action = "SK_UPDATE_SAMPLING_HISTOGRAM";
			summarizeJSON.samplingHistogramRequest = getSampledDataParameters(2);
			webJson.summarize = summarizeJSON;
			//alert(JSON.stringify(webJson));
			if (summarizeJSON.samplingHistogramRequest != null)
				doAjax("/ajaxHandler", JSON.stringify(webJson), updateSet, reportError);	// nothing to do, later refresh the list of captured patterns 
		}
	};
	updateSet3Button = document.createElement("button");
	updateSet3Button.innerText = "Histogram";
	updateSet3Button.id = "UpdateSet3ButtonID";
	updateSet3Button.readonly = true;
	updateSet3Button.setAttribute('class','summButtonEnabledInRow');
	updateSet3Button.onclick = updateSet3ButtonOn;
	
    searchTable = document.createElement("table");
    searchTable.width = "100%";
    searchTable.id = "searchTableID";
    searchTable.setAttribute('class','summselectiontable');

    var tds14 = document.createElement('td'); 
    tds14.colSpan = "8";
    tds14.appendChild(searchTable);
    row13.appendChild(tds14);
    
    document.getElementById('tsSamplingParameterID').onchange = function() {
		var selectedParameter = document.getElementById('tsSamplingParameterID').value;
		document.getElementById('samplingStartDateID').value = getConfigValue(selectedParameter, nodeConfig, 'start');
		document.getElementById('samplingEndDateID').value = getConfigValue(selectedParameter, nodeConfig, 'end');
		samplingDataType.innerText = getConfigValue(selectedParameter, nodeConfig, 'datatype');
	};

}

function pointQuery(result, textStatus, jQxhr) {
	var ajaxJson = JSON.parse(result);
	sketchJSON.spPointResult = ajaxJson.summarize.spPointResult;
	spPointQueryResult.value = sketchJSON.spPointResult;
	messagePanel.text("OK");
}


function rangeQuery(result, textStatus, jQxhr) {
	var ajaxJson = JSON.parse(result);
	sketchJSON.spRangeResult = ajaxJson.summarize.spRangeResult;
	spRangeQueryResult.value = sketchJSON.spRangeResult;
	messagePanel.text("OK");
}

function inverseQuery(result, textStatus, jQxhr) {
	var ajaxJson = JSON.parse(result);
	sketchJSON.spInverseResult = ajaxJson.summarize.spInverseResult;
	spInverseQueryResult.value = sketchJSON.spInverseResult;
	messagePanel.text("OK");
}


function updateStatistics(result, textStatus, jQxhr) {
	//alert(result);
	var ajaxJson = JSON.parse(result);
	sketchJSON.spMin = ajaxJson.summarize.spMin;
	sketchJSON.spMax = ajaxJson.summarize.spMax;
	sketchJSON.spFirst = ajaxJson.summarize.spFirst;
	sketchJSON.spLast = ajaxJson.summarize.spLast;
	sketchJSON.spCount = ajaxJson.summarize.spCount;
	sketchJSON.spTopkValues = ajaxJson.summarize.spTopkValues;
	spMin.value = sketchJSON.spMin;
	spMax.value = sketchJSON.spMax;
	spFirst.value = sketchJSON.spFirst;
	spLast.value = sketchJSON.spLast;
	spCount.value = sketchJSON.spCount;
	if (typeof sketchJSON.spTopkValues != 'undefined') {
		var displayString = "";
		for (var i =0; i< sketchJSON.spTopkValues.length; i++)
			displayString += ((i==0)?"":",") + sketchJSON.spTopkValues[i];
		spFrequentItems.value = displayString;
	}
	messagePanel.text("OK");
}

function updateHistogram(result, textStatus, jQxhr) {
	//alert(result);
	var ajaxJson = JSON.parse(result);
	sketchJSON.spHistogram = ajaxJson.summarize.spHistogram;
	setChartView(sketchJSON.spHistogram,390,150,490,120,0,'Sketch Data Distribution for Node ' + sketchJSON.ln + " " + sketchJSON.spParameter, 'summhisttitlemain');
	messagePanel.text("OK");
}


function getPostBackParameters() {
	sketchJSON.cluster = currentCluster;
	sketchJSON.ln = activeLn;
	sketchJSON.spParameter = document.getElementById('tsParameterID').value;
	sketchJSON.dataType = document.getElementById('dataTypeID').value;
	sketchJSON.spPointQuery = document.getElementById('pointQueryID').value;
	sketchJSON.spRangeQuery1 = document.getElementById('range1ID').value;
	sketchJSON.spRangeQuery2= document.getElementById('range2ID').value;
	sketchJSON.spInverseQuery = document.getElementById('inverseQueryID').value;
	sketchJSON.bins = document.getElementById('spBinsID').value;
	postbackJSON = JSON.parse(JSON.stringify(sketchJSON));
	postbackJSON.spHistogram = [];
	postbackJSON.sampledDataHistogram = [];
	return postbackJSON;
}

function getSampledDataParameters(index) {
	if (typeof cb[index] == 'undefined')
		return null;
	var config = searchSelections[+cb[index].getAttribute("index")];
	samplingHistogramRequest = new Object();
	samplingHistogramRequest.ln = config.ln;
	samplingHistogramRequest.parameter = config.parameter;
	samplingHistogramRequest.start = config.start;
	samplingHistogramRequest.end = config.end;
	samplingHistogramRequest.bins = document.getElementById('sampleBinsID').value;
	samplingHistogramRequest.sampleSize = document.getElementById('sampleSizeID').value;
	samplingHistogramRequest.samplingMethod = "Random w/o Replacement";
	samplingHistogramRequest.datatype = document.getElementById('samplingDataTypeID').value;
	samplingHistogramRequest.index = "" + index;
	samplingHistogramRequest.bins = document.getElementById('sampleBinsID').value;
	return samplingHistogramRequest;
}

function updateSet(result, textStatus, jQxhr) {
	var ajaxJson = JSON.parse(result);
	sketchJSON.samplingHistogramRequest = ajaxJson.summarize.samplingHistogramRequest;
	sketchJSON.spHistogram = ajaxJson.summarize.spHistogram;
	sketchJSON.sampledDataHistogram =  ajaxJson.summarize.sampledDataHistogram;
	var index = +ajaxJson.summarize.samplingHistogramRequest.index;
	setChartView(sketchJSON.spHistogram,120,70,(500 + index*120),320,(1 + index),'Node ' + sketchJSON.samplingHistogramRequest.ln + " " 
			+ sketchJSON.samplingHistogramRequest.parameter + " from Sketch", 'summhisttitle');
	setChartView(sketchJSON.sampledDataHistogram,120,70,(500 + index*120),445,(4 + index),'Node ' + sketchJSON.samplingHistogramRequest.ln 
			+ " " + sketchJSON.samplingHistogramRequest.parameter + " from Sample", 'summhisttitle');
	messagePanel.text("OK");
}

function addSelectionTable() {
	if (typeof searchTable != 'undefined')
		searchTable.innerHTML = "";
	
	var htmlTable = document.getElementById('searchTableID');
    var htmlHeader = document.createElement("thead");
    htmlHeader.className = 'summselectiontableheader';
    htmlHeader.id = 'searchTableHeaderID';
    htmlTable.appendChild(htmlHeader);
    var th1 = document.createElement('th'); 
    th1.innerHTML = "";
    th1.className = 'summselectiontablecell';
    htmlHeader.appendChild(th1);
    //th1.width = '10px';
    var th2 = document.createElement('th'); 
    th2.innerHTML = "Ln";
    th2.className = 'summselectiontablecell';
    htmlHeader.appendChild(th2);
    //th2.width = '10px';
    var th3 = document.createElement('th'); 
    th3.innerHTML = "Id";
    th3.className = 'summselectiontablecell';
    htmlHeader.appendChild(th3);
    //th3.width = '65px';
    var th4 = document.createElement('th'); 
    th4.innerHTML = "Start";
    th4.className = 'summselectiontablecell';
    htmlHeader.appendChild(th4);
    //th4.width = '80px';
    var th5 = document.createElement('th'); 
    th5.innerHTML = "End";
    th5.className = 'summselectiontablecell';
    htmlHeader.appendChild(th5);
    //th5.width = '80px';
	
    var th6 = document.createElement('th'); 
    th6.innerHTML = "";
    th6.className = 'summselectiontablecell';
    htmlHeader.appendChild(th6);
    //th5.width = '80px';
    
	for (var i=0; i< searchSelections.length; i++)
		addTableRow(i,searchSelections[i].ln,searchSelections[i].parameter,searchSelections[i].start,searchSelections[i].end);

}

function addTableRow(index, ln, id, start, end) {
	var button = null;
	if (index == 0)
		button = updateSet1Button;
	else if (index == 1)
		button = updateSet2Button;
	else if (index == 2)
		button = updateSet3Button;
	
	if (searchSelections.length > 3)
		return;
	
	var htmlTable = document.getElementById('searchTableID');
	
	var row = htmlTable.insertRow(index);
    row.id = 'searchTableRowID' + index;
    htmlTable.appendChild(row);
    if (index % 2 == 0)
    	row.className = 'summselectiontablerow';
    else
    	row.className = 'summselectiontablerowalt';
    
    var th1 = document.createElement('td'); 
    cb[index] = createButtonCheckBox('cbID' + index, 'rowenabled', 'rowdisabled');
    cb[index].setAttribute("index",'' + index);
    //cb[rowIndex].onchange = createCheckboxFunction(cb[index]);
    //if (selected)
    //	cb[rowIndex].checked = true;
    th1.appendChild(cb[index]);
    th1.className = 'summselectiontablecell';
    row.appendChild(th1);
    
    var th2 = document.createElement('td'); 
    th2.innerHTML = ln;
    th2.className = 'summselectiontablecell';
    row.appendChild(th2);
    
    var th3 = document.createElement('td'); 
    th3.innerHTML = id;
    th3.className = 'summselectiontablecell';
    row.appendChild(th3);

    var th4 = document.createElement('td'); 
    th4.innerHTML = start;
    th4.className = 'summselectiontablecell';
    row.appendChild(th4);

    var th5 = document.createElement('td'); 
    th5.innerHTML = end;
    th5.className = 'summselectiontablecell';
    row.appendChild(th5);
    
    var th6 = document.createElement('td'); 
    th6.appendChild(button);
    th6.className = 'summselectiontablecell';
    row.appendChild(th6);
}

function clearChartView(graphIndex) {
	if (typeof spBar[graphIndex] != 'undefined')
		spBar[graphIndex].remove();
	if (typeof spHistXaxisLabel[graphIndex] != 'undefined')
		spHistXaxis[graphIndex].remove();
	if (typeof spHistXaxis[graphIndex] != 'undefined')
		spHistXaxis[graphIndex].remove();
	if (typeof spHistXaxisGroup[graphIndex] != 'undefined')
		spHistXaxis[graphIndex].remove();
	if (typeof spHistYaxisLabel[graphIndex] != 'undefined')
		spHistYaxis[graphIndex].remove();
	if (typeof spHistYaxis[graphIndex] != 'undefined')
		spHistYaxis[graphIndex].remove();
	if (typeof spHistYaxisGroup[graphIndex] != 'undefined')
		spHistYaxis[graphIndex].remove();
	if (typeof graphTitle[graphIndex] != 'undefined')
		graphTitle[graphIndex].remove();
}

function setChartView(histogram, width, height, xloc, yloc, graphIndex, title, titlefont) {
	
	if (!histogram || histogram.length == 0)
		return;
	
	clearChartView(graphIndex);
	var w = 1;
	var x = d3.scale.linear().range([0, width-50]);
	var y = d3.scale.linear().range([height, 0]);
	var xAxis = d3.svg.axis().scale(x).orient("bottom");
	var yAxis = d3.svg.axis().scale(y).orient("left");
	
	x.domain(d3.extent(histogram, function(d,i) { return +d.binnum; }));
	y.domain(d3.extent(histogram, function(d,i) { return (+d.bincount==0)?0:(Math.log(+d.bincount)/Math.LN10); }));
	
	spBar[graphIndex] = svgContainer.selectAll("bar").data(histogram).enter().append("g")
		.attr('class',(graphIndex<=3)?'summhistsketch':'summhistsampled')
		.attr("transform", function(d) { return "translate(" + (xloc + x(+d.binnum)) + "," + (yloc + y((+d.bincount==0)?0:(Math.log(+d.bincount)/Math.LN10))) + ")"; });
	spBar[graphIndex].append("rect").attr("x", 1).attr("width", x(w)).attr("height", function(d) { return height - y((+d.bincount==0)?0:(Math.log(+d.bincount)/Math.LN10)); });
	
	spHistXaxisGroup[graphIndex] = svgContainer.append("g").attr("class", "x axis");
	spHistXaxis[graphIndex] = spHistXaxisGroup[graphIndex].call(xAxis).attr('class','summhistxaxis').
		attr("transform", "translate(" + xloc + "," + (yloc + height) + ")");
	spHistXaxisLabel[graphIndex] = spHistXaxis[graphIndex].selectAll('text').attr('class','summhistxaxistext');
	
	spHistYaxisGroup[graphIndex] = svgContainer.append("g").attr("transform", "translate(" + xloc + "," + yloc + ")").attr("class", "y axis");
	spHistYaxis[graphIndex] = spHistYaxisGroup[graphIndex].call(yAxis).attr('class','summhistyaxis');
	spHistYaxisLabel[graphIndex] = spHistYaxis[graphIndex].selectAll('text').attr('class','summhistyaxistext').append("text").attr("transform", "rotate(-90)").attr("y", 6)
		.attr("dy", ".71em").style("text-anchor", "end");
	
	graphTitle[graphIndex] = svgContainer.append("text").attr("x",((graphIndex==0)?(xloc+180):(xloc+40))).attr("y",(yloc-10))
		.attr('class',titlefont).text(title); 
}

function createSketchesNodeSelectionFunction(idx) {
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
		var summarizeJSON = new Object();
		summarizeJSON.ln = "" + clusterStatus[i].logicalNumber;
		summarizeJSON.cluster = clusterStatus[i].cluster;
		summarizeJSON.action = "SK_CONFIGURE";
		webJson.summarize = summarizeJSON;
		//alert(JSON.stringify(webJson));
		doAjax("/ajaxHandler", JSON.stringify(webJson), configurationSelectionNotification, reportError);
		
	};
}

function configurationSelectionNotification(result, textStatus, jQxhr) {

	var ajaxJson = JSON.parse(result);
	activeLn = ajaxJson.summarize.ln;
	currentCluster = 
	nodeConfig = ajaxJson.summarize.nodeConfig;
	populateSelect('tsParameterID',getConfigParameterNames(nodeConfig));
	populateSelect('tsSamplingParameterID',getConfigParameterNames(nodeConfig));
	var selectedParameter = document.getElementById('tsParameterID').value;
	if (typeof selectedParameter != 'undefined')
		document.getElementById('dataTypeID').value = getConfigValue(selectedParameter, nodeConfig, 'datatype');
	else
		document.getElementById('dataTypeID').value = "";

	var selectedSamplingParameter = document.getElementById('tsSamplingParameterID').value;
	if (typeof selectedSamplingParameter != 'undefined') {
		document.getElementById('samplingDataTypeID').value = getConfigValue(selectedSamplingParameter, nodeConfig, 'datatype'); 
		document.getElementById('samplingStartDateID').value = getConfigValue(selectedSamplingParameter, nodeConfig, 'start');
		document.getElementById('samplingEndDateID').value = getConfigValue(selectedSamplingParameter, nodeConfig, 'end');
	} else {
		document.getElementById('samplingDataTypeID').value;
		document.getElementById('samplingStartDateID').value = "";
		document.getElementById('samplingEndDateID').value = "";
	}
	notificationPanel.text("Configuration action completed for action " + ajaxJson.summarize.action); 
	messagePanel.text("OK");
	setClusterStatusASync();
}


function addToSearchList() {
	if (typeof activeLn == 'undefined')
		return;
	
	if (getConfigValue(document.getElementById('tsSamplingParameterID').value, nodeConfig, 'isSketched') != "true") {
		alert('This time series has not been sketched.');
		return;
	}
	
	if (!checkDataType(document.getElementById('samplingDataTypeID').value)) {
		alert ("All time series selected must have the same data type");
		return;
	}
	
	if (searchSelections.length == 3) {
		alert("No more than 3 at a time!");
		return;
	}
	var addition = new Object();
	addition.index = searchSelections.length;
	addition.ln = activeLn;
	addition.parameter = document.getElementById('tsSamplingParameterID').value;
	addition.datatype = document.getElementById('samplingDataTypeID').value;
	addition.start = document.getElementById('samplingStartDateID').value;
	addition.end = document.getElementById('samplingEndDateID').value;
	searchSelections[searchSelections.length] = addition;
	
	addSelectionTable();
}

function removeFromSearchList() {
	if (typeof activeLn == 'undefined')
		return;
	
	if (typeof cb == 'undefined')
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
	sketchJSON.searchSelections = searchSelections;
	addSelectionTable();
	clearChartView(1);clearChartView(2);clearChartView(3);clearChartView(4);clearChartView(5);clearChartView(6);
}

function checkDataType(datatype) {
	for (var i=0; i< searchSelections.length; i++)
		if (searchSelections[i].datatype != datatype)
			return false;
	
	return true;
}