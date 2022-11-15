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
	<link rel="stylesheet" href="<c:url value="/static/css/table.css"/>" type="text/css" media="screen, projection" />
	<link rel="stylesheet" href="<c:url value="/static/css/monitoring/database.css"/>" type="text/css" media="screen, projection" />
	
	<script type="text/javascript" src="<c:url value="/static/js/jquery.dataTables.min.js"/>"></script>
	<script type="text/javascript" src="<c:url value="/static/js/history.js"/>"></script>
	<script type="text/javascript" src="<c:url value="/static/js/dataSource.js"/>"></script>
</head>
	<body>
			<div class="prepend-top span-17 colborder" id="content">
				<button id="toggleSidebarButton">&gt;</button>
				<div class="marginLeft marginBottom"> 
					<h2>Data Migration</h2>
					<div id="tabs">
						<ul>
							<li><a href="#tabs-1">Source Database</a></li>
							<li><a href="#tabs-2">Target Database</a></li>
						</ul>
						
						<div id="tabs-1">
							<div id="tableDsSourceWrapper">
								<table id="tableDsSource" data-url="<c:url value="/commercedbsynchac/migrationDataSource/source"/>">
									<thead>
										<tr>
											<th>Property</th>
											<th>Value</th>
										</tr>
									</thead>
									<tbody>
								      				
									</tbody>
								</table>
							</div>
							<button id="buttonDsSourceValidate" data-url="<c:url value="/commercedbsynchac/migrationDataSource/source/validate"/>">Validate Connection</button>
						</div>
						<div id="tabs-2">
							<div id="tableDsTargetWrapper">
								<table id="tableDsTarget" data-url="<c:url value="/commercedbsynchac/migrationDataSource/target"/>">
									<thead>
										<tr>
											<th>Property</th>
											<th>Value</th>
										</tr>
									</thead>
									<tbody>

									</tbody>
								</table>
							</div>
							<button id="buttonDsTargetValidate" data-url="<c:url value="/commercedbsynchac/migrationDataSource/target/validate"/>">Validate Connection</button>
						</div>
					</div>			
				</div>
			</div>
			


	</body>
</html>

