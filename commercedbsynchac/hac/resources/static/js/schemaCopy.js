/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

var targetSchemaDiffTable;
var sourceSchemaDiffTable;
var buttonMigrateSchemaPreview = "Calculating Diff Preview";
var buttonMigrateSchema = "Execute script";
var buttonGenerateSchemaScript = "Generate Schema Script";
var sqlQueryEditor;

$(document).ready(function() {
	$( "#tabs" ).tabs({
		activate: function(event, ui) {
		}
	});


    $("#sqlQueryWrapper").resizable().height("250px").width("100%");
    sqlQueryEditor = CodeMirror.fromTextArea(document.getElementById("sqlQuery"), {
        mode: "text/x-sql",
        lineNumbers: false,
        lineWrapping: true,
        autofocus: true
    });

	$('#buttonGenerateSchemaScript').click(generateSchemaScript);
	$('#buttonMigrateSchemaPreview').click(migrateSchemaPreview);
	$('#buttonMigrateSchema').prop('disabled', true);

	$('#checkboxAccept').change(function() {
       if($(this).is(":checked")) {
	      $('#buttonMigrateSchema').prop('disabled', false);
       } else {
          $('#buttonMigrateSchema').prop('disabled', true);
       }
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

});

function migrateSchemaPreview()
{
	$('#schemaDiffWrapper').fadeOut();
	targetSchemaDiffTable.fnClearTable();
	sourceSchemaDiffTable.fnClearTable();

	$('#buttonMigrateSchemaPreview').html(buttonMigrateSchemaPreview + ' ' +  hac.global.getSpinnerImg());

    const token = $("meta[name='_csrf']").attr("content");
    const url = $('#buttonMigrateSchemaPreview').attr('data-url');

	$.ajax({
		url:url,
		type:'GET',
		headers:{
            'Accept':'application/json',
            'X-CSRF-TOKEN' : token
        },
		success: function(data) {
			debug.log(data);

			$('#buttonMigrateSchemaPreview').html(buttonMigrateSchemaPreview);

			if(data.target.results.length > 0) {
			    targetSchemaDiffTable.fnAddData(data.target.results);
			}
			if(data.source.results.length > 0) {
			    sourceSchemaDiffTable.fnAddData(data.source.results);
			}

			$("#schemaDiffWrapper").fadeIn();

		},
		error: hac.global.err
	});
}

function generateSchemaScript()
{
	$('#buttonGenerateSchemaScript').html(buttonGenerateSchemaScript + ' ' +  hac.global.getSpinnerImg());
    $("#checkboxAccept").prop("checked", false);
	$('#buttonMigrateSchema').prop('disabled', true);

    var token = $("meta[name='_csrf']").attr("content");
    var url = $('#buttonGenerateSchemaScript').attr('data-url');

	$.ajax({
		url:url,
		type:'GET',
		headers:{
            'Accept':'text/plain',
            'X-CSRF-TOKEN' : token
        },
		success: function(data) {
            hac.global.notify('Duplicate tables may have been found. Please review generated schema script carefully.');
			sqlQueryEditor.setValue(data);
		    $('#buttonGenerateSchemaScript').html(buttonGenerateSchemaScript);
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


