
/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

let charts = {};

$(document).ready(function () {
    getData();
    window.setInterval(getData, 5000)
});


function getData() {
    var token = $("meta[name='_csrf']").attr("content");
    var url = $('#charts').attr('data-chartDataUrl');
    $.ajax({
        url: url,
        type: 'GET',
        headers: {
            'Accept': 'application/json',
            'X-CSRF-TOKEN': token
        },
        success: function (data) {
            data.forEach(function(metric) {
                if($("#" + getChartId(metric)).length == 0) {
                  createContainer(metric);
                  createChart(metric);
                }
                drawChart(metric);
            });
        },
        error: function(xhr, status, error) {
            console.error('Could not get metric data');
        }
    });

}

function createChart(metric) {
    chartId = getChartId(metric);
    chart = new Chart($('#' + chartId), {
        type: 'doughnut',
        data: {
            datasets: [{
                data: [],
                backgroundColor: [],
                label: metric.name
            }],
            labels: []
        },
        options: {
            legend: {
                display: false,
            },
            tooltips: {
                callbacks: {
                    label: function (tooltipItem, data) {
                        var dataset = data.datasets[tooltipItem.datasetIndex];
                        var meta = dataset._meta[Object.keys(dataset._meta)[0]];
                        var total = meta.total;
                        var currentValue = dataset.data[tooltipItem.index];
                        var percentage = parseFloat((currentValue / total * 100).toFixed(1));
                        return currentValue + ' (' + percentage + '%)';
                    },
                    title: function (tooltipItem, data) {
                        return data.labels[tooltipItem[0].index];
                    }
                },
                backgroundColor: "rgb(255,255,255,0.95)",
                titleFontColor: "rgb(0,0,0)",
                bodyFontColor: "rgb(0,0,0)"
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
            }
        }
    });
    charts[chartId] = chart;
}


function createContainer(metric) {
    size = '100px';
    root = $('#charts');
    wrapper = $('<div>').css('margin-bottom','10px');
    root.append(wrapper);
    title = $('<h4>').text(metric.name);
    canvasContainer = $('<div>').attr('id', getChartId(metric)+'-container').css('text-align','center').css('width',size).css('height',size);
    wrapper.append(title);
    wrapper.append(canvasContainer);
    canvas = $('<canvas>').attr('id', getChartId(metric)).attr('width', size).attr('height', size);
    canvasContainer.append(canvas);
}

function drawChart(metric) {
    //debug.log(metric.primaryValue + " / " + metric.secondaryValue);
    chart = charts[getChartId(metric)];
    chart.data.datasets[0].data = [metric.secondaryValue, metric.primaryValue];
    primaryLabel = metric.primaryValueLabel + ' (' + metric.primaryValueUnit + ')';
    secondaryLabel = metric.secondaryValueLabel + ' (' + metric.secondaryValueUnit + ')';
    chart.data.labels = [secondaryLabel, primaryLabel];
    chart.options.tooltips.enabled = true;

    primaryColor = metric.primaryValueStandardColor;
    secondaryColor = metric.secondaryValueStandardColor;
    if(metric.primaryValue < 0) {
        primaryColor = '#9a9fa6';
        secondaryColor = primaryColor;
        chart.options.tooltips.enabled = false;
    } else {
        if(metric.primaryValueThreshold > 0) {
            if(metric.primaryValue >= metric.primaryValueThreshold) {
                primaryColor = metric.primaryValueCriticalColor;
            }
        }
        if(metric.secondaryValueThreshold > 0) {
            if(metric.secondaryValue >= metric.secondaryValueThreshold) {
                secondaryColor = metric.secondaryValueCriticalColor;
            }
        }
    }

    chart.data.datasets[0].backgroundColor = [secondaryColor, primaryColor];
    chart.update();
}

function getChartId(metric) {
    return 'chart-area-' + metric.metricId;
}


