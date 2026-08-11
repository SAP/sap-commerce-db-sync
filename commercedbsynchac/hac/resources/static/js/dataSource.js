/*
 *  Copyright: 2025 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

var tableDsSource;
var tableDsTarget;
var buttonDsSourceValidate = "Validate Connection";
var buttonDsTargetValidate = "Validate Connection";

$(document).ready(function() {

	tableDsSource = $('#tableDsSource').DataTable({
		"bStateSave": true,
		"bAutoWidth": false,
		"paging": false,
        "filter" : false,
        "info" : false,
        "sorting" : [ [ 0, "asc" ] ]
	});

	tableDsTarget = $('#tableDsTarget').DataTable({
        "bStateSave": true,
        "bAutoWidth": false,
		"paging": false,
        "filter" : false,
        "info" : false,
        "sorting" : [ [ 0, "asc" ] ]
    });

    loadSource();
    loadTarget();

	$( "#tabs" ).tabs({
		activate: function(event, ui) {
			if ( ui.newPanel.attr('id') == 'tabs-1') {

			}
            if ( ui.newPanel.attr('id') == 'tabs-2') {

            }

			//toggleActiveSidebar(ui.newPanel.attr('id').replace(/^.*-/, ''));
		}
	});

	$('#buttonDsSourceValidate').click(validateSource);
	$('#buttonDsTargetValidate').click(validateTarget);



});

function validateSource()
{
	$('#buttonDsSourceValidate').html(buttonDsSourceValidate + ' ' +  hac.global.getSpinnerImg());
    var token = $("meta[name='_csrf']").attr("content");

    var url = $('#buttonDsSourceValidate').attr('data-url');

	$.ajax({
		url:url,
		type:'GET',
		headers:{
            'Accept':'application/json',
            'X-CSRF-TOKEN' : token
        },
		success: function(data) {
			debug.log(data);
			if(data.valid === true) {
		        $('#buttonDsSourceValidate').html("Valid!");
			} else {
                $('#buttonDsSourceValidate').html("Not valid!!");
                $('#connectionException').html(data.exception);
			}
		},
		error: hac.global.err
	});
}

function validateTarget()
{
	$('#buttonDsTargetValidate').html(buttonDsTargetValidate + ' ' +  hac.global.getSpinnerImg());
    var token = $("meta[name='_csrf']").attr("content");

    var url = $('#buttonDsTargetValidate').attr('data-url');

	$.ajax({
		url:url,
		type:'GET',
		headers:{
            'Accept':'application/json',
            'X-CSRF-TOKEN' : token
        },
		success: function(data) {
			debug.log(data);
			if(data.valid === true) {
		        $('#buttonDsTargetValidate').html("Valid!");
			} else {
                $('#buttonDsTargetValidate').html("Not valid!!");
                $('#connectionExceptionTarget').html(data.exception);
			}
		},
		error: hac.global.err
	});
}

function loadSource()
{
	$('#tableDsSourceWrapper').fadeOut();
	tableDsSource.clear().draw();

	//$('#buttonCopyData').html(buttonCopyData + ' ' +  hac.global.getSpinnerImg());
    var token = $("meta[name='_csrf']").attr("content");

    var url = $('#tableDsSource').attr('data-url');

	$.ajax({
		url:url,
		type:'GET',
		headers:{
            'Accept':'application/json',
            'X-CSRF-TOKEN' : token
        },
		success: function(data) {
			debug.log(data);
			tableDsSource.row.add(["profile",data.profile]);
			tableDsSource.row.add(["driver",data.driver]);
			tableDsSource.row.add(["connectionString",data.connectionString]);
			tableDsSource.row.add(["userName",data.userName]);
			tableDsSource.row.add(["password",data.password]);
			tableDsSource.row.add(["schema",data.schema]);
			tableDsSource.row.add(["catalog",data.catalog]);
			tableDsSource.draw();

			$("#tableDsSourceWrapper").fadeIn();
		},
		error: hac.global.err
	});
}

function loadTarget()
{
	$('#tableDsTargetWrapper').fadeOut();
	tableDsTarget.clear().draw();

	//$('#buttonCopyData').html(buttonCopyData + ' ' +  hac.global.getSpinnerImg());
    var token = $("meta[name='_csrf']").attr("content");

    var url = $('#tableDsTarget').attr('data-url');

	$.ajax({
		url:url,
		type:'GET',
		headers:{
            'Accept':'application/json',
            'X-CSRF-TOKEN' : token
        },
		success: function(data) {
			debug.log(data);
			tableDsTarget.row.add(["profile",data.profile]);
			tableDsTarget.row.add(["driver",data.driver]);
			tableDsTarget.row.add(["connectionString",data.connectionString]);
			tableDsTarget.row.add(["userName",data.userName]);
			tableDsTarget.row.add(["password",data.password]);
			tableDsTarget.row.add(["schema",data.schema]);
			tableDsTarget.row.add(["catalog",data.catalog]);
			tableDsTarget.draw();

			$("#tableDsTargetWrapper").fadeIn();
		},
		error: hac.global.err
	});
}
