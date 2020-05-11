// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
function setHomeView() {
	if (pageLoad)
		centeringDiv = d3.select("body").append("xhtml:div");
	if (pageLoad) {
		svgContainer = centeringDiv.append("svg").attr("width", 900).attr("height", 75);
		titleBar1 = svgContainer.append("rect").attr("x", 0).attr("y", 0).attr("width", (1000)).attr("height", (8)).attr("class","titlebar1");
		titleBar2 = svgContainer.append("rect").attr("x", 0).attr("y", 8).attr("width", (1000)).attr("height", (23)).attr("class","titlebar2");
		//clusterRing = svgContainer.append("rect").attr("x", (x-3)).attr("y", (y-3)).attr("width", (w+6)).attr("height", (h+6)).attr("class","ring");
		//clusterRing = svgContainer.append("rect").attr("x", x).attr("y", y).attr("width", w).attr("height", h).attr("class","ring");
		setTitle("");
		setLinks();
	}

 	if (pageLoad)
		pageLoad = false;
}
