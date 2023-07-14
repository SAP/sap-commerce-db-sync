<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ taglib prefix="hac" uri="/WEB-INF/custom.tld" %>
<%@ taglib prefix="form" uri="http://www.springframework.org/tags/form" %>
<%--
  ~  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
  ~  License: Apache-2.0
  ~
  --%>

<html>
<head>
    <title>Copy Schema To SAP Commerce Cloud</title>
    <link rel="stylesheet" href="<c:url value="/static/css/table.css"/>" type="text/css" media="screen, projection"/>
    <link rel="stylesheet" href="<c:url value="/static/css/database.css"/>" type="text/css"
          media="screen, projection"/>
    <link rel="stylesheet" href="<c:url value="/static/css/schemaCopy.css"/>" type="text/css"
          media="screen, projection"/>


    <script type="text/javascript" src="<c:url value="/static/js/jquery.dataTables.min.js"/>"></script>
    <script type="text/javascript" src="<c:url value="/static/js/history.js"/>"></script>
    <script type="text/javascript" src="<c:url value="/static/js/migrationReports.js"/>"></script>
</head>
<body>
<div class="prepend-top span-17 colborder" id="content">
    <button id="toggleSidebarButton">&gt;</button>
    <div class="marginLeft marginBottom">
        <h2>Migration Reports</h2>
        <div id="reportsWrapper">
            <table id="reportsTable">
                <thead>
                <tr>
                    <th>Report id</th>
                    <th>Timestamp</th>
                    <th>Download</th>
                </tr>
                </thead>
                <tbody>

                </tbody>
            </table>
        </div>
    </div>
</div>
</body>
</html>
