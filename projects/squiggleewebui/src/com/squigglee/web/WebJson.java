// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
package com.squigglee.web;

public class WebJson {
	
	private CommonPageJson common;
	private ConfigurePageJson configure;
	private OperatePageJson operate;
	private SketchPageJson summarize;
	private MatchPageJson match;
	
	public CommonPageJson getCommon() {
		return common;
	}
	public void setCommon(CommonPageJson common) {
		this.common = common;
	}
	public ConfigurePageJson getConfigure() {
		return configure;
	}
	public void setConfigure(ConfigurePageJson configure) {
		this.configure = configure;
	}
	public OperatePageJson getOperate() {
		return operate;
	}
	public void setOperate(OperatePageJson operate) {
		this.operate = operate;
	}
	public SketchPageJson getSummarize() {
		return summarize;
	}
	public void setSummarize(SketchPageJson summarize) {
		this.summarize = summarize;
	}
	public MatchPageJson getMatch() {
		return match;
	}
	public void setMatch(MatchPageJson match) {
		this.match = match;
	}
}
