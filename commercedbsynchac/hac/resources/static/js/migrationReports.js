/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

var reportsTable;

$(document).ready(function () {
    reportsTable = $('#reportsTable').dataTable({
        "bStateSave": true,
        "bAutoWidth": false,
        "aLengthMenu": [[10, 25, 50, 100, -1], [10, 25, 50, 100, 'all']]
    });
    loadMigrationReports();
});

function loadMigrationReports() {
    $('#logsWrapper').fadeOut();
    reportsTable.fnClearTable();
    var token = $("meta[name='_csrf']").attr("content");
    var url = "/hac/commercedbsynchac/loadMigrationReports";
    $.ajax({
        url: url,
        type: 'GET',
        headers: {
            'Accept': 'application/json',
            'X-CSRF-TOKEN': token
        },
        success: function (data) {
            if (data.length > 0) {
                data.forEach((report) => {
                    let strippedMigrationId = report.reportId;
                    reportsTable.fnAddData([
                        strippedMigrationId,
                        report.modifiedTimestamp,
                        '<button onclick=downloadReport('+JSON.stringify(strippedMigrationId)+') id="buttonCopyReport">Download Report</button>'
                    ])
                });
            }
        },
        error: hac.global.err
    });
}

function downloadReport(migrationId) {
    var token = $("meta[name='_csrf']").attr("content");
    var url = "/hac/commercedbsynchac/downloadLogsReport?migrationId="+migrationId;
    $.ajax({
        url: url,
        type: 'GET',
        headers: {
            'X-CSRF-TOKEN': token
        },
        success: function (data) {
            debug.log(data);
            var blob = new Blob([data], {type: "text/plain"});
            var link = document.createElement("a");
            link.href = window.URL.createObjectURL(blob);
            link.download = migrationId;
            link.click();
        },
        error: hac.global.err
    });
}

