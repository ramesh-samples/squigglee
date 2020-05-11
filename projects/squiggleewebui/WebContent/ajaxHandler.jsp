<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%@ page import="com.google.gson.*,com.google.gson.reflect.*,org.apache.commons.math3.stat.StatUtils,com.squigglee.core.serializers.avro.*,org.joda.time.DateTime,org.joda.time.format.DateTimeFormat,java.io.*,java.util.*,com.squigglee.core.config.*,com.squigglee.core.interfaces.*" %>

<%
// Copyright (c) 2015 SQUIGGLEE LLC All Rights Reserved.
PrintWriter outt = response.getWriter();
Gson gson = new Gson();
com.squigglee.web.ActionHandler ah = new com.squigglee.web.ActionHandler();
String requestJsonPayload = request.getParameterMap().keySet().iterator().next().toString();
com.squigglee.web.WebJson pageJson = new com.squigglee.web.WebJson();
pageJson = gson.fromJson(requestJsonPayload, com.squigglee.web.WebJson.class);
ah.handle(pageJson);
outt.print(gson.toJson(pageJson));
%>