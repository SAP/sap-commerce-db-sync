/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

var targetSchemaDiffTable;
var sourceSchemaDiffTable;
var buttonMigrateSchema = "Execute script";
var buttonStopSchemaPreview = "Stop"
var sqlQueryEditor;

const token = document.querySelector('meta[name="_csrf"]').content;
let currentSchemaDifferenceId;
let pollInterval;

$(document).ready(function() {
    $("#sqlQueryWrapper").resizable().height("250px").width("100%");
    sqlQueryEditor = CodeMirror.fromTextArea(document.getElementById("sqlQuery"), {
        mode: "text/x-sql",
        lineNumbers: false,
        lineWrapping: true,
        autofocus: true
    });

    $( "#tabs" ).tabs({
        activate: function(event, ui) {
            sqlQueryEditor.refresh();
        }
    });

    $('#buttonStartSchemaPreview').click(migrateSchemaPreview);
	$('#buttonStopSchemaPreview').click(abortSchemaPreview);
	$('#buttonStopSchemaPreview').prop('disabled', true);
	$('#buttonMigrateSchema').prop('disabled', true);

	$('#checkboxAccept').change(function() {
      $('#buttonMigrateSchema').prop('disabled', !$(this).is(":checked"));
    });

	// tab 1
	targetSchemaDiffTable = $('#targetSchemaDiffTable').dataTable({
		"bStateSave": true,
		"bAutoWidth": false,
		"aLengthMenu" : [[10,25,50,100,-1], [10,25,50,100,'all']]
	});
    sourceSchemaDiffTable = $('#sourceSchemaDiffTable').dataTable({
        "bStateSave": true,
        "bAutoWidth": false,
        "aLengthMenu" : [[10,25,50,100,-1], [10,25,50,100,'all']]
    });

	// We do not want to submit form using standard http
	$("#schemaSqlForm").submit(function() {
		return false;
	});

    $('#schemaSqlForm').validate({
       submitHandler: migrateSchema
    });

    const url = $('#latestSchemaStatus').attr('data-url');

    $.ajax({
        url: url,
        type: 'GET',
        headers: {
            'Accept': 'application/json',
            'X-CSRF-TOKEN': token
        },
        success: function (status) {
            if (status.status === 'RUNNING') {
                monitorSchemaStatus(status.schemaDifferenceId)
            } else {
                updateStatus(status);
                fillResult(status);
            }
        },
        error: function(xhr, status, error) {
            console.error('Could not get status data');
        }
    });
});

function migrateSchemaPreview() {
    const url = $('#buttonStartSchemaPreview').attr('data-url');

    $.ajax({
        url:url,
        type:'GET',
        headers:{
            'Accept':'text/plain',
            'X-CSRF-TOKEN' : token
        },
        success: function (schemaDifferenceId) {
            monitorSchemaStatus(schemaDifferenceId);
        },
        error: hac.global.err
    });
}

function monitorSchemaStatus(schemaDifferenceId) {
    currentSchemaDifferenceId = schemaDifferenceId;
    targetSchemaDiffTable.fnClearTable();
    sourceSchemaDiffTable.fnClearTable();
    sqlQueryEditor.setValue("");

    $('#buttonStartSchemaPreview').prop('disabled', true);
    $('#buttonStopSchemaPreview').prop('disabled', false);
    $('#checkboxAccept').prop('disabled', true);
    $('#checkboxAccept').prop('checked', false);
    $('#buttonMigrateSchema').prop('disabled', true);

    doPoll();
    pollInterval = setInterval(doPoll, 1000);
}

function doPoll() {
    var url = $('#schemaPreviewStatus').attr('data-url');

    $.ajax({
        url: url,
        type: 'GET',
        data: {
            schemaDifferenceId: currentSchemaDifferenceId,
        },
        headers: {
            'Accept': 'application/json',
            'X-CSRF-TOKEN': token
        },
        success: function (status) {
            updateStatus(status);
            if (status.completed) {
                clearInterval(pollInterval);
                $('#buttonStartSchemaPreview').prop('disabled', false);
                $('#buttonStopSchemaPreview').html(buttonStopSchemaPreview);
                $('#buttonStopSchemaPreview').prop('disabled', true);
                $('#checkboxAccept').prop('disabled', false);

                if (!status.failed) {
                    fillResult(status)
                }
            }
        },
        error: function(xhr, status, error) {
            console.error('Could not get status data');
        }
    });
}

function updateStatus(status) {
    const statusContainer = document.getElementById('schemaPreviewStatus');
    const timeContainer = document.getElementById('schemaPreviewTime');


    const statusSummary = document.createElement('dl');
    statusSummary.classList.add("summary");
    let dt = document.createElement('dt')
    let dd = document.createElement('dd')
    dt.innerText = "ID";
    statusSummary.appendChild(dt);
    dd.innerText = status.schemaDifferenceId;
    statusSummary.appendChild(dd);
    dt = document.createElement("dt");
    dt.innerText = "Status";
    statusSummary.appendChild(dt);
    dd = document.createElement("dd");
    dd.classList.add('status');
    statusSummary.appendChild(dd);
    if (status.aborted) {
        dd.innerText = "Aborted";
        dd.classList.add("failed")
    } else if (status.failed) {
        dd.innerText = "Failed";
        dd.classList.add("failed");
    } else if (status.completed) {
        dd.innerText = "Completed";
        dd.classList.add("completed")
    } else {
        dd.innerHTML = `In Progress... <br/>(last update: ${formatEpoch(status.lastUpdateEpoch)})`
    }
    empty(statusContainer);
    statusContainer.appendChild(statusSummary);

    const timeSummary = document.createElement("dl");
    timeSummary.innerHTML =
        `<dt>Start</dt><dd>${formatEpoch(status.startEpoch)}</dd>` +
        `<dt>End</dt><dd>${formatEpoch(status.endEpoch)}</dd>` +
        `<dt>Duration</dt><dd>${formatDuration(status.startEpoch, status.endEpoch)}</dd>`;
    empty(timeContainer);
    timeContainer.appendChild(timeSummary);
}

function empty(element) {
    while (element.firstChild) {
        element.removeChild(element.lastChild);
    }
}

function formatEpoch(epoch) {
    if (epoch) {
        return new Date(epoch).toISOString();
    } else {
        return "<span class=\"placeholder\">N/A</span>";
    }
}

function formatDuration(startEpoch, endEpoch) {
    if(!startEpoch || !endEpoch) {
        return "<span class=\"placeholder\">N/A</span>";
    } else {
        let sec_num = (endEpoch - startEpoch) / 1000;
        let hours   = Math.floor(sec_num / 3600);
        let minutes = Math.floor((sec_num - (hours * 3600)) / 60);
        let seconds = sec_num - (hours * 3600) - (minutes * 60);
        if (hours   < 10) {hours   = "0"+hours;}
        if (minutes < 10) {minutes = "0"+minutes;}
        if (seconds < 10) {seconds = "0"+seconds;}
        return hours+':'+minutes+':'+seconds;
    }
}

function fillResult(status) {
    targetSchemaDiffTable.fnClearTable();
    sourceSchemaDiffTable.fnClearTable();

    if(status.diffResult.target.results.length > 0) {
        targetSchemaDiffTable.fnAddData(status.diffResult.target.results.map((result) => [result[1], result[2]]));
    }
    if(status.diffResult.source.results.length > 0) {
        sourceSchemaDiffTable.fnAddData(status.diffResult.source.results.map((result) => [result[1], result[2]]));
    }

    sqlQueryEditor.setValue(status.sqlScript);
}

function abortSchemaPreview() {
    const url = $('#buttonStopSchemaPreview').attr('data-url');

    $.ajax({
        url:url,
        type:'POST',
        headers:{
            'Accept':'text/plain',
            'X-CSRF-TOKEN' : token
        }, success() {
            $('#buttonStopSchemaPreview').prop('disabled', true);
            $('#buttonStopSchemaPreview').html(buttonStopSchemaPreview + ' ' +  hac.global.getSpinnerImg());
        },
        error: hac.global.err
    });
}

function migrateSchema()
{
    if(sqlQueryEditor.getValue().length <= 1){
        return false;
    }
    $('#buttonMigrateSchema').html(buttonMigrateSchema + ' ' +  hac.global.getSpinnerImg());
    $('#spinnerWrapper').show();
    var token = $("meta[name='_csrf']").attr("content");

    var url = $('#buttonMigrateSchema').attr('data-url');

    // Prepare data object
    var dataObject = {
        sqlQuery : sqlQueryEditor.getValue(),
        accepted : $('#checkboxAccept').is(":checked")
    };

    $.ajax({
        url:url,
        type:'POST',
        data: dataObject,
        headers:{
            'Accept':'text/plain',
            'X-CSRF-TOKEN' : token
        },
        success: function(data) {
            $('#spinnerWrapper').hide();
            $('#buttonMigrateSchema').html(buttonMigrateSchema);
            sqlQueryEditor.setValue(data);
        },
        error: hac.global.err
    });

}
