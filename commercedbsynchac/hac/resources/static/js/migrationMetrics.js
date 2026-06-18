
/*
 *  Copyright: 2025 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
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
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false,
                },
                tooltip: {
                    enabled: false,
                    external: externalTooltipHandler,
                    callbacks: {
                        title: (items) => items[0].label,
                        label: (context) => {
                            const dataset = context.dataset.data;
                            const value = context.parsed;
                            const total = dataset[0] + dataset[1];
                            const percentage = total > 0 ? (value / total * 100).toFixed(1) : '0.0';
                            const m = context.chart.metric || {};
                            // dataset order is [secondaryValue, primaryValue] — see drawChart
                            const unit = context.dataIndex === 0 ? m.secondaryValueUnit : m.primaryValueUnit;
                            return `${value}${unit ? ' ' + unit : ''} (${percentage}%)`;
                        }
                    }
                }
            }
        }
    });
    charts[chartId] = chart;
}

function getOrCreateTooltipEl() {
    let el = document.getElementById('chartjs-tooltip');
    if (el) {
        return el;
    }
    el = document.createElement('div');
    el.id = 'chartjs-tooltip';
    el.style.position = 'absolute';
    el.style.pointerEvents = 'none';
    el.style.background = 'rgba(255,255,255,0.95)';
    el.style.color = 'rgb(0,0,0)';
    el.style.border = '1px solid rgba(0,0,0,0.15)';
    el.style.borderRadius = '4px';
    el.style.padding = '6px 8px';
    el.style.font = '12px sans-serif';
    el.style.boxShadow = '0 2px 6px rgba(0,0,0,0.15)';
    el.style.transition = 'opacity 0.1s ease';
    el.style.opacity = '0';
    el.style.zIndex = '1000';
    el.style.whiteSpace = 'nowrap';
    document.body.appendChild(el);
    return el;
}

function externalTooltipHandler(context) {
    const {chart, tooltip} = context;
    const el = getOrCreateTooltipEl();

    if (tooltip.opacity === 0) {
        el.style.opacity = '0';
        return;
    }

    const titleLines = tooltip.title || [];
    const bodyLines = (tooltip.body || []).map(b => b.lines).flat();

    let html = '';
    titleLines.forEach(t => {
        html += '<div style="font-weight:bold;margin-bottom:2px;">' + escapeHtml(t) + '</div>';
    });
    bodyLines.forEach(b => {
        html += '<div>' + escapeHtml(b) + '</div>';
    });
    el.innerHTML = html;

    const rect = chart.canvas.getBoundingClientRect();
    el.style.opacity = '1';
    el.style.left = (window.pageXOffset + rect.left + tooltip.caretX + 10) + 'px';
    el.style.top = (window.pageYOffset + rect.top + tooltip.caretY + 10) + 'px';
}

function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c]);
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
    chart.metric = metric;
    chart.data.datasets[0].data = [metric.secondaryValue, metric.primaryValue];
    primaryLabel = metric.primaryValueLabel + ' (' + metric.primaryValueUnit + ')';
    secondaryLabel = metric.secondaryValueLabel + ' (' + metric.secondaryValueUnit + ')';
    chart.data.labels = [secondaryLabel, primaryLabel];
    chart.options.plugins.tooltip.external = externalTooltipHandler;

    primaryColor = metric.primaryValueStandardColor;
    secondaryColor = metric.secondaryValueStandardColor;
    if(metric.primaryValue < 0) {
        primaryColor = '#9a9fa6';
        secondaryColor = primaryColor;
        chart.options.plugins.tooltip.external = () => {
            const el = document.getElementById('chartjs-tooltip');
            if (el) el.style.opacity = '0';
        };
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


