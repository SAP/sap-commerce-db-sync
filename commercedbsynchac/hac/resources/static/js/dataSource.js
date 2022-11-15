/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

var tableDsSource;
var tableDsTarget;
var buttonDsSourceValidate = "Validate Connection";
var buttonDsTargetValidate = "Validate Connection";

$(document).ready(function() {

	tableDsSource = $('#tableDsSource').dataTable({
		"bStateSave": true,
		"bAutoWidth": false,
		"aLengthMenu" : [[10,25,50,100,-1], [10,25,50,100,'all']]
	});

	tableDsTarget = $('#tableDsTarget').dataTable({
        "bStateSave": true,
        "bAutoWidth": false,
        "aLengthMenu" : [[10,25,50,100,-1], [10,25,50,100,'all']]
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
			}
		},
		error: hac.global.err
	});
}

function loadSource()
{
	$('#tableDsSourceWrapper').fadeOut();
	tableDsSource.fnClearTable();
	
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
			tableDsSource.fnAddData(["profile",data.profile]);
			tableDsSource.fnAddData(["driver",data.driver]);
			tableDsSource.fnAddData(["connectionString",data.connectionString]);
			tableDsSource.fnAddData(["userName",data.userName]);
			tableDsSource.fnAddData(["password",data.password]);
			tableDsSource.fnAddData(["schema",data.schema]);
			tableDsSource.fnAddData(["catalog",data.catalog]);

			$("#tableDsSourceWrapper").fadeIn();
		},
		error: hac.global.err
	});				
}
	
function loadTarget()
{
	$('#tableDsTargetWrapper').fadeOut();
	tableDsTarget.fnClearTable();

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
			tableDsTarget.fnAddData(["profile",data.profile]);
			tableDsTarget.fnAddData(["driver",data.driver]);
			tableDsTarget.fnAddData(["connectionString",data.connectionString]);
			tableDsTarget.fnAddData(["userName",data.userName]);
			tableDsTarget.fnAddData(["password",data.password]);
			tableDsTarget.fnAddData(["schema",data.schema]);
			tableDsTarget.fnAddData(["catalog",data.catalog]);

			$("#tableDsTargetWrapper").fadeIn();
		},
		error: hac.global.err
	});
}