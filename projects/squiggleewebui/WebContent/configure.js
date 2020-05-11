// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
var nodeConfig = [];
var counter = 0;
var dataConfigTable,indexConfigTable,sketchConfigTable, dataConfigLayout, sketchConfigLayout, indexConfigLayout;
var cb = [];
var ptrncb = [];
var skchcb = [];
var rowInterval = 15;
var offset = 120;
var dataTypeDD = [];
var newdataTypeField;
var addDataConfigToClusterButton,addDataConfigToClusterButtonOn,removeDataConfigToClusterButton,removeDataConfigToClusterButtonOn;
var addIndexConfigToClusterButton,addIndexConfigToClusterButtonOn,removeIndexConfigToClusterButton,removeIndexConfigToClusterButtonOn;
var addSketchConfigToClusterButton,addSketchConfigToClusterButtonOn,removeSketchConfigToClusterButton,removeSketchConfigToClusterButtonOn;

function setConfigurationView() {

	setRectangleView('Configuration');
	//override the basic class selection function 
	for (var i=0; i< nodes.length; i++)
		nodes[i].on("click",createConfigureNodeSelectionFunction(i));
	
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
		setDataTypes();
		setSketchTypes();
		setFrequencyTypes();
		setDataControls();
		setIndexControls();
		setSketchControls();
	}
	setDataConfigTable();
	setIndexConfigTable();
	setSketchConfigTable();
	
	if (pageLoad)
		pageLoad = false;
}

function setSketchControls() {
	sketchConfigLayout = svgContainer.append("foreignObject").attr("width", 370).attr("height", 250).attr("y",320).attr("x",(460))
	.html("<table width=\"370px\" id=\"sketchConfigLayoutID\" ></table>");
	var layout = document.getElementById('sketchConfigLayoutID');
	layout.setAttribute('class','dataconfiglayout');
	
	var row0 = layout.insertRow(0);
	row0.id = 'sketchConfigLayoutRowID0';
	var row1 = layout.insertRow(1);
    row1.id = 'sketchConfigLayoutRowID1';
    var row2 = layout.insertRow(2);
    row2.id = 'sketchConfigLayoutRowID2';
    var row3 = layout.insertRow(3);
    row3.id = 'sketchConfigLayoutRowID3';
    var row4 = layout.insertRow(4);
    row4.id = 'sketchConfigLayoutRowID4';
    var row5 = layout.insertRow(5);
    row5.id = 'sketchConfigLayoutRowID5';
    var row6 = layout.insertRow(6);
    row6.id = 'sketchConfigLayoutRowID6';
    var row7 = layout.insertRow(7);
    row7.id = 'sketchConfigLayoutRowID7';
    
    var titleLabel = document.createElement("span");
    titleLabel.innerHTML = "<p>Sketch Configuration<p>";
    titleLabel.setAttribute('class','dataconfigtabletitle');
    
    var tsIdLabel = document.createElement("span");
    tsIdLabel.innerHTML = "Time Series ID";
    tsIdLabel.setAttribute("class","controllabel");
    var sketchTypesLabel = document.createElement("span");
    sketchTypesLabel.innerHTML = "Sketch Type";
    sketchTypesLabel.setAttribute("class","controllabel");
    var sketchCardinalityLabel = document.createElement("span");
    sketchCardinalityLabel.innerHTML = "Cardinality";
    sketchCardinalityLabel.setAttribute("class","controllabel");
    var sketchScalarLabel = document.createElement("span");
    sketchScalarLabel.innerHTML = "Scalar";
    var indexScalarLabel = document.createElement("span");
    indexScalarLabel.innerHTML = "Scalar";
    indexScalarLabel.setAttribute("class","controllabel");
    var sketchTopkLabel = document.createElement("span");
    sketchTopkLabel.innerHTML = "Top k";
    sketchTopkLabel.setAttribute("class","controllabel");
    var sketchWidthLabel = document.createElement("span");
    sketchWidthLabel.innerHTML = "Counter Width";
    sketchWidthLabel.setAttribute("class","controllabel");
    var sketchDepthLabel = document.createElement("span");
    sketchDepthLabel.innerHTML = "Counter Depth";
    sketchDepthLabel.setAttribute("class","controllabel");
    
    var tsIdSelect = document.createElement("select");
    tsIdSelect.type = "text";
    tsIdSelect.id = "newSketchConfigParameterID";
    
    var sketchTypesSelect = document.createElement("select");
    sketchTypesSelect.type = "text";
    sketchTypesSelect.id = "newSketchTypeID";
    
    var sketchCardinalityBox = document.createElement("input");
    sketchCardinalityBox.type = "text";
    sketchCardinalityBox.size = 15;
    sketchCardinalityBox.id = "newSketchSizeID";
    
    var sketchScalarBox = document.createElement("input");
    sketchScalarBox.type = "text";
    sketchScalarBox.size = 15;
    sketchScalarBox.id = "newSketchScalarID";
    
    var sketchTopkBox = document.createElement("input");
    sketchTopkBox.type = "text";
    sketchTopkBox.size = 15;
    sketchTopkBox.id = "newSketchTopkID";
    
    var sketchWidthBox = document.createElement("input");
    sketchWidthBox.type = "text";
    sketchWidthBox.size = 15;
    sketchWidthBox.id = "newSketchWidthID";
    
    var sketchDepthBox = document.createElement("input");
    sketchDepthBox.type = "text";
    sketchDepthBox.size = 15;
    sketchDepthBox.id = "newSketchDepthID";
    
    var sketchAddButton = document.createElement("input");
    sketchAddButton.type = "command";
    sketchAddButton.value = "Add";
    sketchAddButton.readonly = true;
    sketchAddButton.id = "addSketchConfigButtonID";
    sketchAddButton.setAttribute('class','cfButtonEnabled');
    addSketchConfigToClusterButtonOn = function() {
		if (typeof activeLn != 'undefined') { 
			var webJson = new Object();
			var configureJSON = getAddSketchPostBackParameters();
			configureJSON.action = "CF_ADD_SKETCH";
			webJson.configure = configureJSON;
			//alert(JSON.stringify(webJson));
			doAjax("/ajaxHandler", JSON.stringify(webJson), configurationOperationNotification, reportError);
		}
	};
	sketchAddButton.onclick = addSketchConfigToClusterButtonOn;
    
    var sketchRemoveButton = document.createElement("input");
    sketchRemoveButton.type = "command";
    sketchRemoveButton.value = "Remove";
    sketchRemoveButton.readonly = true;
    sketchRemoveButton.id = "removeSketchConfigButtonID";
    sketchRemoveButton.setAttribute('class','cfButtonEnabled');
    removeSketchConfigToClusterButtonOn = function() {
		if (typeof activeLn != 'undefined') {
			var webJson = new Object();
			var configureJSON = getDropSketchPostBackParameters();
			if (typeof configureJSON.nodeConfig == 'undefined' || configureJSON.nodeConfig.length == 0)
				return;
			configureJSON.action = "CF_DROP_SKETCH";
			webJson.configure = configureJSON;
			//alert(JSON.stringify(webJson));
			doAjax("/ajaxHandler", JSON.stringify(webJson), configurationOperationNotification, reportError);
		}
	};
	sketchRemoveButton.onclick = removeSketchConfigToClusterButtonOn;
    
    
    var td0 = document.createElement('td'); 
    td0.colSpan = "6";
    td0.appendChild(titleLabel);
    row0.appendChild(td0);
    
    var td1 = document.createElement('td'); 
    td1.colSpan = "2";
    td1.appendChild(tsIdLabel);
    row1.appendChild(td1);
    
    var td2 = document.createElement('td'); 
    td2.colSpan = "2";
    td2.appendChild(sketchTypesLabel);
    row1.appendChild(td2);
    
    var td3 = document.createElement('td'); 
    td3.colSpan = "2";
    td3.appendChild(sketchCardinalityLabel);
    row1.appendChild(td3);
    
    var td4 = document.createElement('td'); 
    td4.colSpan = "2";
    td4.appendChild(tsIdSelect);
    row2.appendChild(td4);
    
    var td5 = document.createElement('td'); 
    td5.colSpan = "2";
    td5.appendChild(sketchTypesSelect);
    row2.appendChild(td5);
    
    var td6 = document.createElement('td'); 
    td6.colSpan = "2";
    td6.appendChild(sketchCardinalityBox);
    row2.appendChild(td6);
    
    row3.appendChild(getBlankColumn(1));
    
    var td7 = document.createElement('td'); 
    td7.colSpan = "1";
    td7.appendChild(sketchScalarLabel);
    row3.appendChild(td7);
    
    var td8 = document.createElement('td'); 
    td8.colSpan = "1";
    td8.appendChild(sketchTopkLabel);
    row3.appendChild(td8);
    
    var td9 = document.createElement('td'); 
    td9.colSpan = "1";
    td9.appendChild(sketchWidthLabel);
    row3.appendChild(td9);
    
    var td10 = document.createElement('td'); 
    td10.colSpan = "1";
    td10.appendChild(sketchDepthLabel);
    row3.appendChild(td10);
    
    row3.appendChild(getBlankColumn(1));
    
    row4.appendChild(getBlankColumn(1));
    
    var td11 = document.createElement('td'); 
    td11.colSpan = "1";
    td11.appendChild(sketchScalarBox);
    row4.appendChild(td11);
    
    var td12 = document.createElement('td'); 
    td12.colSpan = "1";
    td12.appendChild(sketchTopkBox);
    row4.appendChild(td12);
    
    var td13 = document.createElement('td'); 
    td13.colSpan = "1";
    td13.appendChild(sketchWidthBox);
    row4.appendChild(td13);
    
    var td14 = document.createElement('td'); 
    td14.colSpan = "1";
    td14.appendChild(sketchDepthBox);
    row4.appendChild(td14);
    
    row4.appendChild(getBlankColumn(1));

    var td15 = document.createElement('td'); 
    td15.colSpan = "6";
    td15.setAttribute("text-align","right");
    td15.appendChild(sketchAddButton);
    td15.appendChild(getBlankSpan());
    td15.appendChild(getBlankSpan());
    td15.appendChild(getBlankSpan());
    td15.appendChild(getBlankSpan());
    td15.appendChild(getBlankSpan());
    td15.appendChild(sketchRemoveButton);
    row5.appendChild(td15);

    row6.appendChild(getBlankColumn(6));
    
    sketchConfigTable = document.createElement("table");
    sketchConfigTable.width = "100%";
    sketchConfigTable.id = "sketchConfigTableID";
    sketchConfigTable.setAttribute('class','dataconfigtable');
    
    var td16 = document.createElement('td'); 
    td16.colSpan = "6";
    td16.appendChild(sketchConfigTable);
    row7.appendChild(td16);
	
    document.getElementById('newSketchTypeID').onchange = function() {
		if (document.getElementById('newSketchTypeID').value == "EX") {
			document.getElementById('newSketchWidthID').value = "";
			document.getElementById('newSketchWidthID').disabled = true;
			document.getElementById('newSketchDepthID').value = "";
			document.getElementById('newSketchDepthID').disabled = true;
		}
		if (document.getElementById('newSketchTypeID').value == "CM") {
			document.getElementById('newSketchWidthID').disabled = false;
			document.getElementById('newSketchDepthID').disabled = false;
		}
	};
}

function setSketchConfigTable() {
	var checkstring = [];
	if (typeof skchcb != 'undefined') {
    	for (var j=0; j< skchcb.length; j++) {
    		if (typeof skchcb[j] != 'undefined' && skchcb[j].getAttribute('checked') == 'true')  
    			checkstring[j] = skchcb[j].getAttribute("parameter") + ":" + 
    			skchcb[j].getAttribute("index");
    		else
    			checkstring[j] = "";
    		//alert(checkstring[j]);
    	}
	}
	if (typeof sketchConfigTable != 'undefined')
		sketchConfigTable.innerHTML = "";
	setSketchConfigTableHeader();

	skchcb = [];
    var rowIndex = 0;
    if (typeof nodeConfig != 'undefined') {
	    for (var j=0; j< nodeConfig.length; j++) {
	    	var indexString = nodeConfig[j].indexes;
	    	if (typeof indexString == 'undefined')
	    		continue;
	    	var indices = indexString.split(";");
	    	for (var idx = 0; idx < indices.length; idx++)
	    		if (indices[idx].indexOf("skch") == 0)
	    			setSketchConfigTableRow(nodeConfig[j].parameter,rowIndex++, checkstring, indices[idx]);
	    }
    }
}

function setSketchConfigTableRow(tsId, rowIndex, priors, indexName) {
	var selected = false;
	if (typeof priors != 'undefined') {
		for (var p=0; p<priors.length; p++)
			if (priors[p] == (tsId + ":" + indexName) ) {
				selected = true;
				break;
			}
	}
	
	var htmlTable = document.getElementById('sketchConfigTableID');
	
	var row = htmlTable.insertRow(rowIndex);
    row.id = 'sketchConfigTableRowID' + rowIndex;
    htmlTable.appendChild(row);
    if (rowIndex % 2 == 0)
    	row.className = 'dataconfigtablerow';
    else
    	row.className = 'dataconfigtablerowalt';
    
    var th1 = document.createElement('td'); 
    skchcb[rowIndex] = createButtonCheckBox('sketchConfigCbID' + rowIndex, 'rowenabled', 'rowdisabled');
    skchcb[rowIndex].setAttribute("parameter",'' + tsId);
    skchcb[rowIndex].setAttribute("index",'' + indexName);
    
    if (selected) {
    	skchcb[rowIndex].setAttribute("checked","true");
    	skchcb[rowIndex].setAttribute('class', 'rowenabled');
    } else {
    	skchcb[rowIndex].setAttribute("checked","false");
    	skchcb[rowIndex].setAttribute('class', 'rowdisabled');
    }
    
    th1.appendChild(skchcb[rowIndex]);
    th1.className = 'indexconfigtablecell';
    row.appendChild(th1);
    
    var th2 = document.createElement('td'); 
    th2.innerHTML = tsId;
    th2.className = 'indexconfigtablecell';
    row.appendChild(th2);
    
    var th3 = document.createElement('td'); 
    th3.innerHTML = indexName;
    th3.className = 'indexconfigtablecell';
    row.appendChild(th3);
}

function setSketchConfigTableHeader() {
	var htmlTable = document.getElementById('sketchConfigTableID');
    var htmlHeader = document.createElement("thead");
    htmlHeader.className = 'dataconfigtableheader';
    htmlHeader.id = 'sketchConfigTableHeaderID';
    htmlTable.appendChild(htmlHeader);
    var th1 = document.createElement('th'); 
    th1.innerHTML = "";
    th1.className = 'indexconfigtablecell';
    htmlHeader.appendChild(th1);
    th1.width = '8px';
    var th2 = document.createElement('th'); 
    th2.innerHTML = "Time Series ID";
    th2.className = 'indexconfigtablecell';
    htmlHeader.appendChild(th2);
    th2.width = '60px';
    var th3 = document.createElement('th'); 
    th3.innerHTML = "Sketch ID";
    th3.className = 'indexconfigtablecell';
    htmlHeader.appendChild(th3);
    th3.width = '60px';
}


function setIndexControls() {
	indexConfigLayout = svgContainer.append("foreignObject").attr("width", 370).attr("height", 250).attr("y",75).attr("x",(460))
	.html("<table width=\"370px\" id=\"indexConfigLayoutID\" ></table>");
	var layout = document.getElementById('indexConfigLayoutID');
	layout.setAttribute('class','dataconfiglayout');
	
	var row0 = layout.insertRow(0);
	row0.id = 'indexConfigLayoutRowID0';
	var row1 = layout.insertRow(1);
    row1.id = 'indexConfigLayoutRowID1';
    var row2 = layout.insertRow(2);
    row2.id = 'indexConfigLayoutRowID2';
    var row3 = layout.insertRow(3);
    row3.id = 'indexConfigLayoutRowID3';
    var row4 = layout.insertRow(4);
    row4.id = 'indexConfigLayoutRowID4';
    var row5 = layout.insertRow(5);
    row5.id = 'indexConfigLayoutRowID5';
    var row6 = layout.insertRow(6);
    row6.id = 'indexConfigLayoutRowID6';
    var row7 = layout.insertRow(7);
    row7.id = 'indexConfigLayoutRowID7';
    var row8 = layout.insertRow(8);
    row8.id = 'indexConfigLayoutRowID8';
    
    var titleLabel = document.createElement("span");
    titleLabel.innerHTML = "<p>Index Configuration<p>";
    titleLabel.setAttribute('class','dataconfigtabletitle');
    
    var tsIdLabel = document.createElement("span");
    tsIdLabel.innerHTML = "Time Series ID";
    tsIdLabel.setAttribute("class","controllabel");
    var indexDimensionsLabel = document.createElement("span");
    indexDimensionsLabel.innerHTML = "Dimensionality";
    indexDimensionsLabel.setAttribute("class","controllabel");
    var indexProjectionsLabel = document.createElement("span");
    indexProjectionsLabel.innerHTML = "Projections";
    indexProjectionsLabel.setAttribute("class","controllabel");
    var indexSizeLabel = document.createElement("span");
    indexSizeLabel.innerHTML = "Size";
    indexSizeLabel.setAttribute("class","controllabel");;
    var indexScalarLabel = document.createElement("span");
    indexScalarLabel.innerHTML = "Scalar";
    indexScalarLabel.setAttribute("class","controllabel");
    var indexBucketsLabel = document.createElement("span");
    indexBucketsLabel.innerHTML = "Bucket Width";
    indexBucketsLabel.setAttribute("class","controllabel");
    
    var tsIdSelect = document.createElement("select");
    tsIdSelect.type = "text";
    tsIdSelect.id = "newIndexConfigParameterID";
    
    var indexDimensionsBox = document.createElement("input");
    indexDimensionsBox.type = "text";
    indexDimensionsBox.size = 25;
    indexDimensionsBox.id = "newIndexConfigDimensionsID";
    
    var indexProjectionsBox = document.createElement("input");
    indexProjectionsBox.type = "text";
    indexProjectionsBox.size = 25;
    indexProjectionsBox.id = "newIndexConfigProjectionsID";
    
    var indexSizeBox = document.createElement("input");
    indexSizeBox.type = "text";
    indexSizeBox.size = 25;
    indexSizeBox.id = "newIndexConfigSizeID";
    
    var indexScalarBox = document.createElement("input");
    indexScalarBox.type = "text";
    indexScalarBox.size = 25;
    indexScalarBox.id = "newIndexConfigScalarID";
    
    var indexBucketsBox = document.createElement("input");
    indexBucketsBox.type = "text";
    indexBucketsBox.size = 25;
    indexBucketsBox.id = "newIndexConfigBucketsID";
    
    var indexAddButton = document.createElement("input");
    indexAddButton.type = "command";
    indexAddButton.value = "Add";
    indexAddButton.readonly = true;
    indexAddButton.id = "addIndexConfigButtonID";
    indexAddButton.setAttribute('class','cfButtonEnabled');
    addIndexConfigToClusterButtonOn = function() {
		if (typeof activeLn != 'undefined') {
			
			var webJson = new Object();
			var configureJSON = getAddIndexPostBackParameters();
			configureJSON.action = "CF_ADD_INDEX";
			webJson.configure = configureJSON;
			//alert(JSON.stringify(webJson));
			doAjax("/ajaxHandler", JSON.stringify(webJson), configurationOperationNotification, reportError);
		}
	};
	indexAddButton.onclick = addIndexConfigToClusterButtonOn;
    
    var indexRemoveButton = document.createElement("input");
    indexRemoveButton.type = "command";
    indexRemoveButton.value = "Remove";
    indexRemoveButton.readonly = true;
    indexRemoveButton.id = "removeIndexConfigButtonID";
    indexRemoveButton.setAttribute('class','cfButtonEnabled');
    removeIndexConfigToClusterButtonOn = function() {
		if (typeof activeLn != 'undefined') {
			
			var webJson = new Object();
			var configureJSON = getDropIndexPostBackParameters();
			if (typeof configureJSON.nodeConfig == 'undefined' || configureJSON.nodeConfig.length == 0)
				return;
			configureJSON.action = "CF_DROP_INDEX";
			webJson.configure = configureJSON;
			//alert(JSON.stringify(webJson));
			doAjax("/ajaxHandler", JSON.stringify(webJson), configurationOperationNotification, reportError);
		}
	};
	indexRemoveButton.onclick = removeIndexConfigToClusterButtonOn;
    
    var td0 = document.createElement('td'); 
    td0.colSpan = "6";
    td0.appendChild(titleLabel);
    row0.appendChild(td0);
    
    var td1 = document.createElement('td'); 
    td1.colSpan = "2";
    td1.appendChild(tsIdLabel);
    row1.appendChild(td1);
    
    var td2 = document.createElement('td'); 
    td2.colSpan = "2";
    td2.appendChild(indexDimensionsLabel);
    row1.appendChild(td2);
    
    var td3 = document.createElement('td'); 
    td3.colSpan = "2";
    td3.appendChild(indexProjectionsLabel);
    row1.appendChild(td3);
    
    var td4 = document.createElement('td'); 
    td4.colSpan = "2";
    td4.appendChild(tsIdSelect);
    row2.appendChild(td4);
    
    var td5 = document.createElement('td'); 
    td5.colSpan = "2";
    td5.appendChild(indexDimensionsBox);
    row2.appendChild(td5);
    
    var td6 = document.createElement('td'); 
    td6.colSpan = "2";
    td6.appendChild(indexProjectionsBox);
    row2.appendChild(td6);
    
    row3.appendChild(getBlankColumn(6));
    
    var td7 = document.createElement('td'); 
    td7.colSpan = "2";
    td7.appendChild(indexSizeLabel);
    row4.appendChild(td7);
    
    var td8 = document.createElement('td'); 
    td8.colSpan = "2";
    td8.appendChild(indexScalarLabel);
    row4.appendChild(td8);
    
    var td9 = document.createElement('td'); 
    td9.colSpan = "2";
    td9.appendChild(indexBucketsLabel);
    row4.appendChild(td9);
    
    var td10 = document.createElement('td'); 
    td10.colSpan = "2";
    td10.appendChild(indexSizeBox);
    row5.appendChild(td10);
    
    var td11 = document.createElement('td'); 
    td11.colSpan = "2";
    td11.appendChild(indexScalarBox);
    row5.appendChild(td11);
    
    var td12 = document.createElement('td'); 
    td12.colSpan = "2";
    td12.appendChild(indexBucketsBox);
    row5.appendChild(td12);
  
    var td13 = document.createElement('td'); 
    td13.colSpan = "6";
    td13.setAttribute("text-align","right");
    td13.appendChild(indexAddButton);
    td13.appendChild(getBlankSpan());
    td13.appendChild(getBlankSpan());
    td13.appendChild(getBlankSpan());
    td13.appendChild(getBlankSpan());
    td13.appendChild(getBlankSpan());
    td13.appendChild(indexRemoveButton);
    row6.appendChild(td13);
    
    row7.appendChild(getBlankColumn(6));
    
    indexConfigTable = document.createElement("table");
    indexConfigTable.width = "100%";
    indexConfigTable.id = "indexConfigTableID";
    indexConfigTable.setAttribute('class','dataconfigtable');
    
    var td15 = document.createElement('td'); 
    td15.colSpan = "6";
    td15.appendChild(indexConfigTable);
    row8.appendChild(td15);
}


function setIndexConfigTable() {
	var checkstring = [];
	if (typeof ptrncb != 'undefined') {
    	for (var j=0; j< ptrncb.length; j++) {
    		if (typeof ptrncb[j] != 'undefined' && ptrncb[j].getAttribute('checked') == 'true')
    			checkstring[j] = ptrncb[j].getAttribute("parameter") + ":" + 
    			ptrncb[j].getAttribute("index");
    		else
    			checkstring[j] = "";
    		//alert(checkstring[j]);
    	}
	}
	if (typeof indexConfigTable != 'undefined')
		indexConfigTable.innerHTML = "";
	setIndexConfigTableHeader();

    ptrncb = [];
    var rowIndex = 0;
    if (typeof nodeConfig != 'undefined')	{
	    for (var j=0; j< nodeConfig.length; j++) {
	    	var indexString = nodeConfig[j].indexes;
	    	if (typeof indexString == 'undefined')
	    		continue;
	    	var indices = indexString.split(";");
	    	for (var idx = 0; idx < indices.length; idx++)
	    		if (indices[idx].indexOf("ptrn") == 0)
	    			setIndexConfigTableRow(nodeConfig[j].parameter,rowIndex++, checkstring, indices[idx]);
	    }
    }
}


function setIndexConfigTableRow(tsId, rowIndex, priors, indexName) {
	var selected = false;
	if (typeof priors != 'undefined') {
		for (var p=0; p<priors.length; p++)
			if (priors[p] == (tsId + ":" + indexName) ) {
				selected = true;
				break;
			}
	}
	
	var htmlTable = document.getElementById('indexConfigTableID');
	
	var row = htmlTable.insertRow(rowIndex);
    row.id = 'indexConfigTableRowID' + rowIndex;
    htmlTable.appendChild(row);
    if (rowIndex % 2 == 0)
    	row.className = 'dataconfigtablerow';
    else
    	row.className = 'dataconfigtablerowalt';
    
    var th1 = document.createElement('td'); 
    ptrncb[rowIndex] = createButtonCheckBox('indexConfigCbID' + rowIndex, 'rowenabled', 'rowdisabled');
    ptrncb[rowIndex].setAttribute("parameter",'' + tsId);
    ptrncb[rowIndex].setAttribute("index",'' + indexName);
    
    
    if (selected) {
    	ptrncb[rowIndex].setAttribute("checked","true");
    	ptrncb[rowIndex].setAttribute('class', 'rowenabled');
    } else {
    	ptrncb[rowIndex].setAttribute("checked","false");
    	ptrncb[rowIndex].setAttribute('class', 'rowdisabled');
    }
    
    th1.appendChild(ptrncb[rowIndex]);
    th1.className = 'dataconfigtablecell';
    row.appendChild(th1);
    
    var th2 = document.createElement('td'); 
    th2.innerHTML = tsId;
    th2.className = 'dataconfigtablecell';
    row.appendChild(th2);
    
    var th3 = document.createElement('td'); 
    th3.innerHTML = indexName;
    th3.className = 'dataconfigtablecell';
    row.appendChild(th3);
}

function setIndexConfigTableHeader() {
	var htmlTable = document.getElementById('indexConfigTableID');
    var htmlHeader = document.createElement("thead");
    htmlHeader.className = 'dataconfigtableheader';
    htmlHeader.id = 'indexConfigTableHeaderID';
    htmlTable.appendChild(htmlHeader);
    var th1 = document.createElement('th'); 
    th1.innerHTML = "";
    th1.className = 'indexconfigtablecell';
    htmlHeader.appendChild(th1);
    th1.width = '8px';
    var th2 = document.createElement('th'); 
    th2.innerHTML = "Time Series ID";
    th2.className = 'indexconfigtablecell';
    htmlHeader.appendChild(th2);
    th2.width = '60px';
    var th3 = document.createElement('th'); 
    th3.innerHTML = "Index ID";
    th3.className = 'indexconfigtablecell';
    htmlHeader.appendChild(th3);
    th3.width = '60px';
}


function setDataControls() {
	dataConfigLayout = svgContainer.append("foreignObject").attr("width", 370).attr("height", 450).attr("y",75).attr("x",(60))
	.html("<table width=\"370px\" id=\"dataConfigLayoutID\" ></table>");
	var layout = document.getElementById('dataConfigLayoutID');
	layout.setAttribute('class','dataconfiglayout');
	
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
    
    var titleLabel = document.createElement("span");
    titleLabel.innerHTML = "<p>Data Configuration<p>";
    titleLabel.setAttribute('class','dataconfigtabletitle');
    
    var tsIdLabel = document.createElement("span");
    tsIdLabel.innerHTML = "Time Series ID";
    tsIdLabel.setAttribute("class","controllabel");
    var tsDtypeLabel = document.createElement("span");
    tsDtypeLabel.setAttribute("class","controllabel");
    tsDtypeLabel.innerHTML = "Data Type";
    var tsFreqLabel = document.createElement("span");
    tsFreqLabel.innerHTML = "Frequency";
    tsFreqLabel.setAttribute("class","controllabel");
    var tsStartLabel = document.createElement("span");
    tsStartLabel.innerHTML = "Start:";
    tsStartLabel.setAttribute("class","controllabel");
    var tsEndLabel = document.createElement("span");
    tsEndLabel.innerHTML = "End:";
    tsEndLabel.setAttribute("class","controllabel");
    
    var tsIdBox = document.createElement("input");
    tsIdBox.type = "text";
    tsIdBox.size = 25;
    tsIdBox.id = "newDataConfigParameterID";
    
    var tsStartBox = document.createElement("input");
    tsStartBox.type = "text";
    tsStartBox.size = 25;
    tsStartBox.id = "newDataConfigStartDateID";
    
    var tsEndBox = document.createElement("input");
    tsEndBox.type = "text";
    tsEndBox.size = 25;
    tsEndBox.id = "newDataConfigEndDateID";
    
    var tsDtypeSelect = document.createElement("select");
    tsDtypeSelect.id = "newDataTypeID";
    
    var tsFreqSelect = document.createElement("select");
    tsFreqSelect.id = "newDataConfigFrequencyID";
    
    var tsAddButton = document.createElement("input");
    tsAddButton.type = "command";
    tsAddButton.value = "Add";
    tsAddButton.id = "addDataConfigButtonID";
    tsAddButton.readonly = true;
    tsAddButton.setAttribute('class','cfButtonEnabled');
    addDataConfigToClusterButtonOn = function() {
		if (typeof activeLn != 'undefined') {
			var webJson = new Object();
			var configureJSON = getAddDataPostBackParameters();
			
			configureJSON.action = "CF_ADD";
			webJson.configure = configureJSON;
			//alert(JSON.stringify(webJson));
			doAjax("/ajaxHandler", JSON.stringify(webJson), configurationOperationNotification, reportError);
		}
	};
    tsAddButton.onclick = addDataConfigToClusterButtonOn;
    
    var tsRemoveButton = document.createElement("input");
    tsRemoveButton.type = "command";
    tsRemoveButton.value = "Remove";
    tsRemoveButton.id = "removeDataConfigButtonID";
    tsRemoveButton.readonly = true;
    tsRemoveButton.setAttribute('class','cfButtonEnabled');
    removeDataConfigToClusterButtonOn = function() {
		if (typeof activeLn != 'undefined') {
			var webJson = new Object();
			var configureJSON = getRemoveDataPostBackParameters();
			if (typeof configureJSON.nodeConfig == 'undefined' || configureJSON.nodeConfig.length == 0)
				return;
			configureJSON.action = "CF_REMOVE";
			webJson.configure = configureJSON;
			//alert(JSON.stringify(webJson));
			doAjax("/ajaxHandler", JSON.stringify(webJson), configurationOperationNotification, reportError);
		}
	};
    tsRemoveButton.onclick = removeDataConfigToClusterButtonOn;
    
    var td0 = document.createElement('td'); 
    td0.colSpan = "6";
    td0.appendChild(titleLabel);
    row0.appendChild(td0);
    
    row1.appendChild(getBlankColumn(1));
    var td1 = document.createElement('td'); 
    td1.colSpan = "1";
    td1.appendChild(tsIdLabel);
    row1.appendChild(td1);
    row1.appendChild(getBlankColumn(1));
    row1.appendChild(getBlankColumn(1));
    var td2 = document.createElement('td'); 
    td2.colSpan = "1";
    td2.appendChild(tsDtypeLabel);
    row1.appendChild(td2);
    
    var td3 = document.createElement('td'); 
    td3.colSpan = "1";
    td3.appendChild(tsFreqLabel);
    row1.appendChild(td3);
    
    row2.appendChild(getBlankColumn(1));
    var td4 = document.createElement('td'); 
    td4.colSpan = "1";
    td4.appendChild(tsIdBox);
    row2.appendChild(td4);
    row2.appendChild(getBlankColumn(1));
    row2.appendChild(getBlankColumn(1));
    var td5 = document.createElement('td'); 
    td5.colSpan = "1";
    td5.appendChild(tsDtypeSelect);
    row2.appendChild(td5);
    
    var td6 = document.createElement('td'); 
    td6.colSpan = "1";
    td6.appendChild(tsFreqSelect);
    row2.appendChild(td6);
    
    populateSelect('newDataTypeID',dataTypes);
    populateSelect('newDataConfigFrequencyID',frequencyTypes);
    
    var td7 = document.createElement('td'); 
    td7.colSpan = "1";
    td7.appendChild(tsStartLabel);
    td7.setAttribute('align','right');
    row3.appendChild(td7);
    
    var td8 = document.createElement('td'); 
    td8.colSpan = "1";
    td8.appendChild(tsStartBox);
    row3.appendChild(td8);
    row3.appendChild(getBlankColumn(1));
    row3.appendChild(getBlankColumn(1));
    row3.appendChild(getBlankColumn(1));
    row3.appendChild(getBlankColumn(1));
    
    var td9 = document.createElement('td'); 
    td9.colSpan = "1";
    td9.appendChild(tsEndLabel);
    td9.setAttribute('align','right');
    row4.appendChild(td9);
    
    var td10 = document.createElement('td'); 
    td10.colSpan = "1";
    td10.appendChild(tsEndBox);
    row4.appendChild(td10);
    row4.appendChild(getBlankColumn(1));
    row4.appendChild(getBlankColumn(1));
    row4.appendChild(getBlankColumn(1));
    row4.appendChild(getBlankColumn(1));
    
    var td11 = document.createElement('td'); 
    td11.colSpan = "6";
    td11.setAttribute("text-align","right");
    td11.appendChild(tsAddButton);
    td11.appendChild(getBlankSpan());
    td11.appendChild(getBlankSpan());
    td11.appendChild(getBlankSpan());
    td11.appendChild(getBlankSpan());
    td11.appendChild(getBlankSpan());
    td11.appendChild(tsRemoveButton);
    row5.appendChild(td11);

    row6.appendChild(getBlankColumn(6));
    
    dataConfigTable = document.createElement("table");
    dataConfigTable.width = "100%";
    dataConfigTable.id = "dataConfigTableID";
    dataConfigTable.setAttribute('class','dataconfigtable');
    
    var td13 = document.createElement('td'); 
    td13.colSpan = "6";
    td13.appendChild(dataConfigTable);
    row7.appendChild(td13);
    
}

function configurationOperationNotification(result, textStatus, jQxhr) {
	//alert(result);
	var ajaxJson = JSON.parse(result);
	activeLn = ajaxJson.configure.ln;
	nodeConfig = ajaxJson.configure.nodeConfig;
	populateSelect('newIndexConfigParameterID',getConfigParameterNames(nodeConfig));
	populateSelect('newSketchConfigParameterID',getConfigParameterNames(nodeConfig));
	notificationPanel.text("Configuration action completed for action " + ajaxJson.action); 
	messagePanel.text("OK").attr("fill","green");
	setClusterStatusASync();
}

function configurationSelectionNotification(result, textStatus, jQxhr) {
	//alert(result);
	var ajaxJson = JSON.parse(result);
	activeLn = ajaxJson.configure.ln;
	//RKR
	currentCluster = ajaxJson.configure.cluster;
	nodeConfig = ajaxJson.configure.nodeConfig;
	document.getElementById('newDataConfigStartDateID').value = ajaxJson.configure.sampleStartOfToday;
	document.getElementById('newDataConfigEndDateID').value = ajaxJson.configure.sampleEndOfToday;
	populateSelect('newIndexConfigParameterID',getConfigParameterNames(nodeConfig));
	populateSelect('newSketchConfigParameterID',getConfigParameterNames(nodeConfig));
	populateSelect('newSketchTypeID',sketchTypes);
	notificationPanel.text("Configuration action completed for action " + ajaxJson.action); 
	messagePanel.text("OK").attr("fill","green");
	setClusterStatusASync();
}

function getAddDataPostBackParameters() {
	var dataConfigRequest = new Object();
	dataConfigRequest.ln = activeLn;
	dataConfigRequest.cluster = currentCluster;
	dataConfigRequest.nodeConfig = [];
	dataConfigRequest.nodeConfig[0] = new Object();
	dataConfigRequest.nodeConfig[0].ln = activeLn;
	dataConfigRequest.nodeConfig[0].cluster = currentCluster;
	dataConfigRequest.nodeConfig[0].parameter = document.getElementById('newDataConfigParameterID').value;
	dataConfigRequest.nodeConfig[0].datatype = document.getElementById('newDataTypeID').value;
	dataConfigRequest.nodeConfig[0].frequency = document.getElementById('newDataConfigFrequencyID').value;
	dataConfigRequest.nodeConfig[0].start = document.getElementById('newDataConfigStartDateID').value;
	dataConfigRequest.nodeConfig[0].end = document.getElementById('newDataConfigEndDateID').value;
	dataConfigRequest.nodeConfig[0].replication = getNodeReplicationProfile();
	return dataConfigRequest;
}

function getRemoveDataPostBackParameters() {
	var dataConfigRequest = new Object();
	dataConfigRequest.ln = activeLn;
	dataConfigRequest.cluster = currentCluster;
	dataConfigRequest.nodeConfig = [];
	for (var i=0; i< cb.length; i++) {
		if (cb[i].getAttribute('checked') != 'true')
			continue;
		var dc = new Object();
		dc.parameter = cb[i].getAttribute("parameter");
		dc.start = cb[i].getAttribute("start");
		dc.end = cb[i].getAttribute("end");
		dc.frequency = cb[i].getAttribute("frequency");
		dc.datatype = cb[i].getAttribute("datatype");
		dc.ln = cb[i].getAttribute("ln");
		dc.cluster = cb[i].getAttribute("cluster");
		if (cb[i].getAttribute("indexes") != 'undefined')
			dc.indexes = cb[i].getAttribute("indexes");
		dataConfigRequest.nodeConfig[dataConfigRequest.nodeConfig.length] = dc;
	}
	return dataConfigRequest;
}

function getAddIndexPostBackParameters() {
	var dataConfigRequest = new Object();
	dataConfigRequest.ln = activeLn;
	dataConfigRequest.cluster = currentCluster;
	dataConfigRequest.nodeConfig = [];
	
	dataConfigRequest.nodeConfig[0] = new Object();
	dataConfigRequest.nodeConfig[0].parameter = document.getElementById('newIndexConfigParameterID').value;
	dataConfigRequest.nodeConfig[0].ln = activeLn;
	dataConfigRequest.nodeConfig[0].cluster = currentCluster;
	dataConfigRequest.nodeConfig[0].indexes = "ptrn_" + document.getElementById('newIndexConfigDimensionsID').value + "_" + 
	document.getElementById('newIndexConfigBucketsID').value + "_" + 
	document.getElementById('newIndexConfigProjectionsID').value + "_" + 
	document.getElementById('newIndexConfigSizeID').value + "_" + 
	document.getElementById('newIndexConfigScalarID').value;

	return dataConfigRequest;
}

function getDropIndexPostBackParameters() {
	var dataConfigRequest = new Object();
	dataConfigRequest.ln = activeLn;
	dataConfigRequest.cluster = currentCluster;
	dataConfigRequest.nodeConfig = [];
	
	for (var i=0; i< ptrncb.length; i++) {
		if (ptrncb[i].getAttribute('checked') != 'true')
			continue;
		var dc = new Object();
		dc.parameter = ptrncb[i].getAttribute("parameter");
		dc.indexes = ptrncb[i].getAttribute("index");
		dc.ln = cb[i].getAttribute("ln");
		dc.cluster = cb[i].getAttribute("cluster");
		dataConfigRequest.nodeConfig[dataConfigRequest.nodeConfig.length] = dc;
	}

	return dataConfigRequest;
}

function getAddSketchPostBackParameters() {
	var dataConfigRequest = new Object();
	dataConfigRequest.ln = activeLn;
	dataConfigRequest.cluster = currentCluster;
	dataConfigRequest.nodeConfig = [];
	
	dataConfigRequest.nodeConfig[0] = new Object();
	dataConfigRequest.nodeConfig[0].parameter = document.getElementById('newSketchConfigParameterID').value;
	dataConfigRequest.nodeConfig[0].ln = activeLn;
	dataConfigRequest.nodeConfig[0].cluster = currentCluster;
	if (document.getElementById('newSketchTypeID').value == "CM")
		dataConfigRequest.nodeConfig[0].indexes = "skch" + document.getElementById('newSketchTypeID').value + "_" 
			+ document.getElementById('newSketchSizeID').value + "_" + 
			document.getElementById('newSketchScalarID').value + "_" + 
			document.getElementById('newSketchTopkID').value + "_" +
			document.getElementById('newSketchWidthID').value + "_" + 
			document.getElementById('newSketchDepthID').value;
	else if (document.getElementById('newSketchTypeID').value == "EX")
		dataConfigRequest.nodeConfig[0].indexes = "skch" + document.getElementById('newSketchTypeID').value + "_" 
		+ document.getElementById('newSketchSizeID').value + "_" + 
		document.getElementById('newSketchScalarID').value + "_" +
		document.getElementById('newSketchTopkID').value;

	return dataConfigRequest;
}

function getDropSketchPostBackParameters() {
	var dataConfigRequest = new Object();
	dataConfigRequest.ln = activeLn;
	dataConfigRequest.cluster = currentCluster;
	dataConfigRequest.nodeConfig = [];
	
	for (var i=0; i< skchcb.length; i++) {
		if (skchcb[i].getAttribute('checked') != 'true')
			continue;
		var dc = new Object();
		dc.parameter = skchcb[i].getAttribute("parameter");
		dc.indexes = skchcb[i].getAttribute("index");
		dc.ln = cb[i].getAttribute("ln");
		dc.cluster = cb[i].getAttribute("cluster");
		dataConfigRequest.nodeConfig[dataConfigRequest.nodeConfig.length] = dc;
	}

	return dataConfigRequest;
}

function getNodeReplicationProfile() {
	var repl = [];
	for (var i = 0; i < clusterStatus.length; i++) {
		var dcName = clusterStatus[i].dataCenter;
		switch (dcName) {
			case "us-east-1":
				dcName = "us-east";
				break;
			case "us-west-1":
				dcName = "us-west";
				break;
			case "us-west-2":
				dcName = "us-west-2";
				break;
			case "eu-west-1":
				dcName = "eu-west";
				break;
			case "eu-central-1":
				dcName = "eu-central";
				break;
			case "ap-northeast-1":
				dcName = "ap-northeast";
				break;
			case "ap-southeast-1":
				dcName = "ap-southeast";
				break;
			case "ap-southeast-2":
				dcName = "ap-southeast-2";
				break;
			
		}
		
		//this here is a EC2 and MultiRegionSnitch hack due to a known bug 
		if (dcName.toLowerCase().indexOf("us-east-1") >= 0)
			dcName = dcName.replace("us-east-1","us-east");
		if (clusterStatus[i].replicaOf == activeLn) {
			if (repl[dcName])
				repl[dcName]++;
			else
				repl[dcName] = 1;
		}
	}
	var rp = "";
	for (var k in repl) {
		if (repl.hasOwnProperty(k)) {
			rp += k + ":" + repl[k] + ";";
	    }
	}
	return rp;
}



function setDataConfigTable() {
	var checkstring = [];
	if (typeof cb != 'undefined') {
    	for (var j=0; j< cb.length; j++) {
    		if (typeof cb[j] != 'undefined' && cb[j].getAttribute('checked') == 'true')
    			checkstring[j] = cb[j].getAttribute("parameter") + ":" + 
    				cb[j].getAttribute("start") + ":" + cb[j].getAttribute("end") + ":" + cb[j].getAttribute("datatype");
    		else
    			checkstring[j] = "";
    		//alert(checkstring[j]);
    	}
	}
	if (typeof dataConfigTable != 'undefined')
		dataConfigTable.innerHTML = "";
	
    setDataConfigTableHeader();
    cb = [];
    var rowIndex = 0;
    if (typeof nodeConfig != 'undefined') {
    	for (var j=0; j< nodeConfig.length; j++)
    		setDataConfigTableRow(j,checkstring, rowIndex++);
    }
}


function setDataConfigTableRow(index, priors, rowIndex) {
	var selected = false;
	if (typeof priors != 'undefined') {
		for (var p=0; p<priors.length; p++)
			if (priors[p] == (nodeConfig[index].parameter + ":" + nodeConfig[index].start + 
					":" + nodeConfig[index].end + ":" + nodeConfig[index].datatype) ) {
				selected = true;
				break;
			}
	}
	
	var htmlTable = document.getElementById('dataConfigTableID');
	
	var row = htmlTable.insertRow(index);
    row.id = 'dataConfigTableRowID' + index;
    htmlTable.appendChild(row);
    if (rowIndex % 2 == 0)
    	row.className = 'dataconfigtablerow';
    else
    	row.className = 'dataconfigtablerowalt';
    
    var th1 = document.createElement('td'); 
    cb[rowIndex] = createButtonCheckBox('dataConfigCbID' + index, 'rowenabled', 'rowdisabled');
    cb[rowIndex].setAttribute("ln",'' + nodeConfig[index].ln);
    cb[rowIndex].setAttribute("cluster",'' + nodeConfig[index].cluster);
    cb[rowIndex].setAttribute("indexes",'' + nodeConfig[index].indexes);
    cb[rowIndex].setAttribute("parameter",'' + nodeConfig[index].parameter);
    cb[rowIndex].setAttribute("start",'' + nodeConfig[index].start);
    cb[rowIndex].setAttribute("end",'' + nodeConfig[index].end);
    cb[rowIndex].setAttribute("datatype",'' + nodeConfig[index].datatype);
    cb[rowIndex].setAttribute("frequency",'' + nodeConfig[index].frequency);
    
    if (selected) {
    	cb[rowIndex].setAttribute("checked","true");
    	cb[rowIndex].setAttribute('class', 'rowenabled');
    } else {
    	cb[rowIndex].setAttribute("checked","false");
    	cb[rowIndex].setAttribute('class', 'rowdisabled');
    }
    
    th1.appendChild(cb[rowIndex]);
    th1.className = 'dataconfigtablecell';
    row.appendChild(th1);
    
    var th2 = document.createElement('td'); 
    th2.innerHTML = nodeConfig[index].parameter;
    th2.className = 'dataconfigtablecell';
    row.appendChild(th2);
    
    var th3 = document.createElement('td'); 
    th3.innerHTML = nodeConfig[index].datatype;
    th3.className = 'dataconfigtablecell';
    row.appendChild(th3);

    var th4 = document.createElement('td'); 
    th4.innerHTML = nodeConfig[index].frequency;
    th4.className = 'dataconfigtablecell';
    row.appendChild(th4);

    var th5 = document.createElement('td'); 
    th5.innerHTML = nodeConfig[index].start;
    th5.className = 'dataconfigtablecell';
    row.appendChild(th5);

    var th6 = document.createElement('td'); 
    th6.innerHTML = nodeConfig[index].end;
    th6.className = 'dataconfigtablecell';
    row.appendChild(th6);
}


function setDataConfigTableHeader() {
	var htmlTable = document.getElementById('dataConfigTableID');
    var htmlHeader = document.createElement("thead");
    htmlHeader.className = 'dataconfigtableheader';
    htmlHeader.id = 'dataConfigTableHeaderID';
    htmlTable.appendChild(htmlHeader);
    var th1 = document.createElement('th'); 
    th1.innerHTML = "";
    th1.className = 'dataconfigtablecell';
    htmlHeader.appendChild(th1);
    th1.width = '8px';
    var th2 = document.createElement('th'); 
    th2.innerHTML = "ID";
    th2.className = 'dataconfigtablecell';
    htmlHeader.appendChild(th2);
    th2.width = '30px';
    var th3 = document.createElement('th'); 
    th3.innerHTML = "Data Type";
    th3.className = 'dataconfigtablecell';
    htmlHeader.appendChild(th3);
    th3.width = '20px';
    var th4 = document.createElement('th'); 
    th4.innerHTML = "Freq";
    th4.className = 'dataconfigtablecell';
    htmlHeader.appendChild(th4);
    th4.width = '20px';
    var th5 = document.createElement('th'); 
    th5.innerHTML = "Start";
    th5.className = 'dataconfigtablecell';
    htmlHeader.appendChild(th5);
    th5.width = '40px';
    var th6 = document.createElement('th'); 
    th6.innerHTML = "End";
    th6.className = 'dataconfigtablecell';
    htmlHeader.appendChild(th6);
    th6.width = '40px';
}


function createConfigureNodeSelectionFunction(idx) {
	var i = idx;
    return function () {
    	//alert(clusterStatus[i]);
    	//no configuration required for replica nodes 
    	if ( 
    			( clusterStatus && (clusterStatus[i].replicaOf != clusterStatus[i].logicalNumber) ) 
    			//|| 
    			//( (clusterStatus[i].logicalNumber == 0) && (clusterStatus[i].dataCenter.toLowerCase().indexOf("local") < 0) ) 
    		)
    		return;

    	clearControls();
    	
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
		var configureJSON = new Object();
		configureJSON.ln = "" + clusterStatus[i].logicalNumber;
		configureJSON.cluster = clusterStatus[i].cluster;
		configureJSON.action = "CF_SET";
		webJson.configure = configureJSON;
		//alert(JSON.stringify(webJson));
		doAjax("/ajaxHandler", JSON.stringify(webJson), configurationSelectionNotification, reportError);
	};
}

function clearControls() {
	if (typeof cb != 'undefined')
		for (var i=0; i<cb.length; i++)
			cb[i].setAttribute('checked','false');
	if (typeof ptrncb != 'undefined')
		for (var i=0; i<ptrncb.length; i++)
			ptrncb[i].setAttribute('checked','false');
	if (typeof skchcb != 'undefined')
		for (var i=0; i<skchcb.length; i++)
			skchcb[i].setAttribute('checked','false');
}



