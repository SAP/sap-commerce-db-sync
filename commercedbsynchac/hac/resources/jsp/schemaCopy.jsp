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
    <link rel="stylesheet" href="<c:url value="/static/css/table.css"/>" type="text/css" media="screen, projection" />
    <link rel="stylesheet" href="<c:url value="/static/css/database.css"/>" type="text/css" media="screen, projection" />
    <link rel="stylesheet" href="<c:url value="/static/css/schemaCopy.css"/>" type="text/css" media="screen, projection" />
    <link rel="stylesheet" href="<c:url value="/static/css/console/codemirror3-custom.css"/>" type="text/css" media="screen, projection" />
    <link rel="stylesheet" href="<c:url value="${useCodeMirrorWebJar ? '/webjars/codemirror/lib' : '/static/css/codemirror3'}/codemirror.css"/>" type="text/css" media="screen, projection" />


    <script type="text/javascript" src="<c:url value="/static/js/jquery.dataTables.min.js"/>"></script>
    <script type="text/javascript" src="<c:url value="/static/js/history.js"/>"></script>
    <script type="text/javascript" src="<c:url value="/static/js/schemaCopy.js"/>"></script>
    <script type="text/javascript" src="<c:url value="${useCodeMirrorWebJar ? '/webjars/codemirror/lib' : '/static/js/codemirror3'}/codemirror.js"/>"></script>
    <script type="text/javascript" src="<c:url value="${useCodeMirrorWebJar ? '/webjars/codemirror/mode/sql' : '/static/js/codemirror3'}/sql.js"/>"></script>
</head>
    <body>
            <div class="prepend-top span-17 colborder" id="content">
                <button id="toggleSidebarButton">&gt;</button>
                <div class="marginLeft marginBottom">
                    <h2>Schema Migration</h2>
                    <div id="tabs">
                        <ul>
                            <li><a href="#tabs-1">Preview Schema Migration</a></li>
                            <li><a href="#tabs-2">Schema Migration</a></li>

                        </ul>
                        <div id="tabs-1">
                            <button id="buttonMigrateSchemaPreview" data-url="<c:url value="/commercedbsynchac/previewSchemaMigration"/>">Preview Schema Migration Changes</button>
                            <div id="schemaDiffWrapper">
                                <h3>Target Schema</h3>
                                <p>Target Schema is missing the following elements which are present in Source Schema </p>
                                <table id="targetSchemaDiffTable">
                                    <thead>
                                        <tr>
                                            <th>Missing Table</th>
                                            <th>Missing Column</th>
                                        </tr>
                                    </thead>
                                    <tbody>

                                    </tbody>
                                </table>
                                <h3>Source Schema</h3>
                                <p>Source Schema is missing the following elements which are present in Target Schema </p>
                                <table id="sourceSchemaDiffTable">
                                    <thead>
                                        <tr>
                                            <th>Missing Table</th>
                                            <th>Missing Column</th>
                                        </tr>
                                    </thead>
                                    <tbody>

                                    </tbody>
                                 </table>
                            </div>
                        </div>
                        <div id="tabs-2">
                            <div class="prepend-top">
                                <h2>Schema Migration Configuration</h2>
                                <c:forEach var="entry" items="${schemaSettings}">
                                  <div>
                                    <input disabled type="checkbox" id="${entry.key}" name="${entry.key}"
                                            <c:if test="${entry.value}">checked="checked"</c:if> >
                                    <label for="${entry.key}">${entry.key}</label>
                                  </div>
                                </c:forEach>
                            </div>
                            <form:form method="post" action="${url}" modelAttribute="schemaSqlForm">
                                <div class="prepend-top">
                                    <h2>Generate SQL Script</h2>
                                    <button id="buttonGenerateSchemaScript" data-url="<c:url value="/commercedbsynchac/generateSchemaScript"/>">Generate Schema Script</button>
                                        <div id="sqlQueryWrapper" class="textarea-container">
                                            <div id="spinnerWrapper">
                                                <img id="spinner" src="<c:url value="/static/img/spinner.gif"/>" alt="spinner"/>
                                            </div>
                                            <form:textarea path="sqlQuery" cols="50" rows="8" />
                                        </div>

                                </div>
                                <div class="prepend-top">
                                    <h2>Execute SQL Script</h2>
                                    <hac:note additionalCssClass="marginBottom">
                                        After the script generation check that your schema differences are correctly reflected by the SQL statements.
                                        The checks may include completeness of 'add' and 'drop' statements as well as the corresponding data types.
                                        Once verified, accept the box below and execute the script. The changes will only affect the target database.
                                    </hac:note>
                                    <form:checkbox id="checkboxAccept" path="accepted" label="I reviewed and verified the script above" />
                                    <button <c:if test="${schemaMigrationDisabled}">disabled</c:if> id="buttonMigrateSchema" data-url="<c:url value="/commercedbsynchac/migrateSchema"/>">Execute script</button>
                                </div>
                            </form:form>
                        </div>
                    </div>
                </div>
            </div>
    </body>
</html>