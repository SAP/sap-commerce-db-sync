/*
 *  Copyright: 2022 SAP SE or an SAP affiliate company and commerce-db-synccontributors.
 *  License: Apache-2.0
 *
 */

'use strict';
(function () {
    function setupMigration() {
        const statusContainer = document.getElementById('copyStatus');
        const summaryContainer = document.getElementById('copySummary');
        const timeContainer = document.getElementById('copyTime');
        const statusUrl = statusContainer.dataset.url;
        const logContainer = document.getElementById("copyLogContainer");
        const reportButton = document.getElementById("buttonCopyReport");
        const dataSourceButton = document.getElementById("buttonDataSourceReport");
        const dataTargetButton = document.getElementById("buttonDataTargetReport") ;
        const reportForm = document.getElementById("formCopyReport");
        const timezoneCheckbox = document.getElementById("timezoneCheckbox")
        const token = document.querySelector('meta[name="_csrf"]').content;
        let lastUpdateTime = Date.UTC(1970, 0, 1, 0, 0, 0);
        let pollInterval;
        let startButtonContentBefore;
        let currentMigrationID;
        let startUrl;
        let stopUrl;

        const startButton = document.getElementById("buttonCopyData")
        const stopButton = document.getElementById("buttonStopCopyData")

        if (startButton && stopButton) {
            startUrl = startButton.dataset.url;
            stopUrl = stopButton.dataset.url;

            startButton.disabled = true;
            startButton.addEventListener('click', copyData);
            stopButton.disabled = true;
            stopButton.addEventListener('click', stopCopy);

            ConfigPanel.initPanel($('#configPanel'));

            startButtonContentBefore = startButton.innerHTML;
        }

        resumeRunning();

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

        function resumeRunning() {
            $.ajax({
                url: '/hac/commercedbsynchac/resumeRunning',
                type: 'GET',
                headers: {
                    'Accept': 'application/json',
                    'X-CSRF-TOKEN': token
                },
                success: function (data) {
                    if (data) {
                        if (data.status === 'RUNNING' && startButton) {
                            startButton.innerHTML = startButtonContentBefore + ' ' + hac.global.getSpinnerImg();

                            if (timezoneCheckbox) {
                                $('#timezoneCheckboxContainer').hide();
                            }
                        }

                        configureStartButton(data.status === 'RUNNING')

                        reportButton.disabled = !(data.status === 'RUNNING');
                        if (dataSourceButton) {
                            dataSourceButton.disabled = !(data.status === 'RUNNING');
                        }
                        if (dataTargetButton) {
                            dataTargetButton.disabled = !(data.status === 'RUNNING');
                        }
                        if (stopButton) {
                            stopButton.disabled = !(data.status === 'RUNNING');
                        }
                        currentMigrationID = data.migrationID;
                        empty(logContainer);
                        updateStatus(data);
                        doPoll();
                        pollInterval = setInterval(doPoll, 5000);
                    } else {
                        configureStartButton(false)
                    }
                },
                error: function (data) {
                    configureStartButton(false)
                }
            });
        }

        function configureStartButton(disable) {
            if (!startButton) return;

            startButton.disabled = disable;

            if (!disable && timezoneCheckbox) {
                startButton.disabled = !$(timezoneCheckbox).is(':checked');
            }
        }

        function copyData() {
            startButtonContentBefore = startButton.innerHTML;
            startButton.innerHTML = startButtonContentBefore + ' ' + hac.global.getSpinnerImg();
            startButton.disabled = true;
            reportButton.disabled = true;
            if (dataSourceButton) {
                dataSourceButton.disabled = true;
            }
            if (dataTargetButton) {
                dataTargetButton.disabled = true;
            }
            stopButton.disabled = false;
            $.ajax({
                url: startUrl,
                type: 'POST',
                data: ConfigPanel.values(),
                headers: {
                    'Accept': 'application/json',
                    'X-CSRF-TOKEN': token,
                },
                success: function (data) {
                    if(data.customException!=null)
                    {
                     hac.global.error(data.customException);

                     stopButton.disabled = true;
                     startButton.innerHTML = startButtonContentBefore;
                     startButton.disabled = false;
                    }
                    else
                    {
                    currentMigrationID = data.migrationID;
                    empty(logContainer);
                    updateStatus(data);
                    doPoll();
                    pollInterval = setInterval(doPoll, 5000);
                    }

                },
                error: function(xht, textStatus, ex) {
                    hac.global.error("Data migration process failed, please check the logs");

                    stopButton.disabled = true;
                    startButton.innerHTML = startButtonContentBefore;

                    configureStartButton(false)
                }
            });
        }

        function stopCopy() {
            stopButton.disabled = true;
            startButton.innerHTML = startButtonContentBefore;

            configureStartButton(false)
            $.ajax({
                url: stopUrl,
                type: 'PUT',
                data: currentMigrationID,
                headers: {
                    'Accept': 'application/json',
                    'X-CSRF-TOKEN': token
                },
                success: function (data) {
                },
                error: hac.global.err
            });
        }

        function updateStatus(status) {
            const statusSummary = document.createElement('dl');
            statusSummary.classList.add("summary");
            let dt = document.createElement('dt')
            let dd = document.createElement('dd')
            dt.innerText = "ID";
            statusSummary.appendChild(dt);
            dd.innerText = status.migrationID;
            statusSummary.appendChild(dd);
            dt = document.createElement("dt");
            dt.innerText = "Status";
            statusSummary.appendChild(dt);
            dd = document.createElement("dd");
            dd.classList.add('status');
            statusSummary.appendChild(dd);
            if (status.failed) {
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

            const progressSummary = document.createElement("dl");
            progressSummary.classList.add("progress");
            progressSummary.innerHTML =
                `<dt>Total</dt><dd class="total">${status.totalTasks}</dd>` +
                `<dt>Completed</dt><dd class="completed">${status.completedTasks}</dd>` +
                `<dt>Failed</dt><dd class="failed">${status.failedTasks}</dd>`;
            empty(summaryContainer);
            summaryContainer.appendChild(progressSummary);

            const timeSummary = document.createElement("dl");
            timeSummary.innerHTML =
                `<dt>Start</dt><dd>${formatEpoch(status.startEpoch)}</dd>` +
                `<dt>End</dt><dd>${formatEpoch(status.endEpoch)}</dd>` +
                `<dt>Duration</dt><dd>${formatDuration(status.startEpoch, status.endEpoch)}</dd>`;
            empty(timeContainer);
            timeContainer.appendChild(timeSummary);
        }

        function doPoll() {
            console.log(new Date(lastUpdateTime).toISOString());
            $.ajax({
                url: statusUrl,
                type: 'GET',
                data: {
                    migrationID: currentMigrationID,
                    since: lastUpdateTime
                },
                headers: {
                    'Accept': 'application/json',
                    'X-CSRF-TOKEN': token
                },
                success: function (status) {
                    // Sticky scroll: https://stackoverflow.com/a/21067431
                    // allow 1px inaccuracy by adding 1
                    const isScrolledToBottom = logContainer.scrollHeight - logContainer.clientHeight <= logContainer.scrollTop + 1
                    writeLogs(status.statusUpdates);
                    if (isScrolledToBottom) {
                        logContainer.scrollTop = logContainer.scrollHeight - logContainer.clientHeight
                    }
                    updateStatus(status);
                    if (status.completed) {
                        if (startButton) {
                            startButton.innerHTML = startButtonContentBefore
                            configureStartButton(false)
                            stopButton.disabled = true;
                        }
                        $(reportForm).children('input[name=migrationId]').val(currentMigrationID);
                        reportButton.disabled = false;
                        if (dataSourceButton) {
                            $(dataSourceButton).siblings('input[name=migrationId]').val(currentMigrationID);
                            dataSourceButton.disabled = false;
                        }
                        if (dataTargetButton) {
                            $(dataTargetButton).siblings('input[name=migrationId]').val(currentMigrationID);
                            dataTargetButton.disabled = false;
                        }
                        clearInterval(pollInterval);
                    }
                },
                error: function(xhr, status, error) {
                    console.error('Could not get status data');
                }
            });
            lastUpdateTime = Date.now();
        }

        function writeLogs(statusUpdates) {
            statusUpdates.forEach(function (entry) {
                let message = `${formatEpoch(entry.lastUpdateEpoch)} | ${entry.pipelinename} | ${entry.targetrowcount} / ${entry.sourcerowcount} | `;
                let p = document.createElement("p");
                if (entry.failure) {
                    message += `FAILED! Reason: ${entry.error}`;
                    p.classList.add("failed");
                }else if (entry.completed) {
                    message += `Completed in ${entry.duration}`;
                    p.classList.add("completed");
                } else {
                    message += "In progress..."
                }
                p.textContent = message;
                logContainer.appendChild(p);
            });
        }
    }

    function domReady(fn) {
        document.addEventListener("DOMContentLoaded", fn);
        if (document.readyState === "interactive" || document.readyState === "complete") {
            fn();
        }
    }

    domReady(setupMigration);
})();

