<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ taglib prefix="hac" uri="/WEB-INF/custom.tld" %>
<%--
  ~  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
  ~  License: Apache-2.0
  ~
  --%>

<html>
<head>
    <title>Migrate Data To SAP Commerce Cloud</title>
    <link rel="stylesheet" href="<c:url value="/static/css/dataCopy.css"/>" type="text/css"
          media="screen, projection"/>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/chartjs/Chart.min.css"/>"/>
    <script type="text/javascript" src="<c:url value="/static/js/history.js"/>"></script>
    <script type="text/javascript" src="<c:url value="/static/js/dataCopy.js"/>"></script>
    <script type="text/javascript" src="<c:url value="/static/js/migrationMetrics.js"/>"></script>
    <script type="text/javascript" src="<c:url value="/static/js/chartjs/Chart.min.js"/>"></script>
    <script type="text/javascript">
        Chart.platform.disableCSSInjection = true;
    </script>

</head>
<body>
<div class="prepend-top span-24" id="content">
    <div id="migrationPanel" class="marginLeft colborder span-17">
        <h2>Data Migration</h2>
        <c:if test="${isIncremental}">
            <hac:note additionalCssClass="marginBottom">
                Incremental mode is enabled. Only rows changed after ${incrementalTimestamp} for specified tables will be copied.
            </hac:note>
        </c:if>
        <div class="clearfix">
            <button id="buttonCopyData" class="control-button" data-url="<c:url value="/commercedbsynchac/copyData"/>">Start</button>
            <button id="buttonStopCopyData" class="control-button" data-url="<c:url value="/commercedbsynchac/abortCopy"/>">Stop</button>
        </div>
        <div class="prepend-top clearfix info marginRight">
            <div class="span-8">
                <dl>
                    <dt>Source Typesystem</dt>
                    <dd><span class="placeholder">${srcTsName}</span></dd>
                    <dt>Target Typesystem</dt>
                    <dd><span class="placeholder">${tgtTsName}</span></dd>
                </dl>
            </div>
            <div class="span-8 append-8">
                <dl>
                    <dt>Source Table Prefix</dt>
                    <dd><span class="placeholder">${srcPrefix}</span></dd>
                    <dt>Target (Migration) Table Prefix</dt>
                    <dd><span class="placeholder">${tgtMigPrefix}</span></dd>
                    <dt>Target (Running System) Table Prefix</dt>
                    <dd><span class="placeholder">${tgtActualPrefix}</span></dd>
                </dl>
            </div>
        </div>
        <div class="prepend-top clearfix status">
            <div id="copyStatus" class="span-8" data-url="<c:url value="/commercedbsynchac/copyStatus"/>">
                <dl>
                    <dt>ID</dt>
                    <dd><span class="placeholder">N/A</span></dd>
                    <dt>Status</dt>
                    <dd><span class="placeholder">N/A</span></dd>
                </dl>
            </div>
            <div id="copySummary" class="span-8">
                <dl>
                    <dt>Total</dt>
                    <dd><span class="placeholder">N/A</span></dd>
                    <dt>Finished</dt>
                    <dd><span class="placeholder">N/A</span></dd>
                    <dt>Failed</dt>
                    <dd><span class="placeholder">N/A</span></dd>
                </dl>
            </div>
            <div id="copyTime" class="span-8 last">
                <dl>
                    <dt>Start</dt>
                    <dd><span class="placeholder">N/A</span></dd>
                    <dt>End</dt>
                    <dd><span class="placeholder">N/A</span></dd>
                    <dt>Duration</dt>
                    <dd><span class="placeholder">N/A</span></dd>
                </dl>
            </div>
        </div>
        <div class="prepend-top">
            <form id="formCopyReport" method="GET" action="<c:url value="/commercedbsynchac/copyReport"/>">
                <input type="hidden" name="migrationId"/>
                <button id="buttonCopyReport" class="inactive" disabled>Download Report</button>
            </form>
        </div>
        <div class="prepend-top" style="visibility:hidden">
            <button id="buttonSwitchPrefix" data-url="<c:url value="/commercedbsynchac/switchPrefix?prefix=${tgtMigPrefix}"/>">Switch Synonym to Prefix ${tgtMigPrefix}</button>
        </div>
        <div class="prepend-top">
            <h2>Migration Log</h2>
            <div id="copyLogContainer">
                <p><span class="placeholder">Migration not started.</span></p>
            </div>
        </div>
    </div>
    <div id="metricsPanel" class="span-5">
        <div id="charts" class="marginLeft" data-chartDataUrl="<c:url value="/commercedbsynchac/metrics"/>" />
    </div>
</div>
</body>
</html>

