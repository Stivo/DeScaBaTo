import {reactive, html} from 'https://esm.sh/@arrow-js/core';
import {formatBytes, byteFormatter, timestampFormatter} from "/js/utils.js";

function deleteRevision(id) {
    fetch(`/api/revision/${id}`, {
        method: 'DELETE'
    })
        .then(res => renderRevisions())
}

function reset(id) {
    fetch(`/api/rollback`, {
        method: 'POST'
    })
        .then(res => renderRevisions())
}

function applyDeletions(id) {
    fetch(`/api/applyDeletions`, {
        method: 'POST'
    })
        .then(res => renderRevisions())
}

function stopServer() {
    fetch(`/api/stopServer`, {
        method: 'POST'
    })
}

function saveSettings(config) {
    console.log(JSON.stringify(config));
    fetch(`/api/retentionPolicy`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(config),
    })
        .then(res => renderRevisions())
}

var config = reactive({
    "latest": 10,
    "hourly": 10,
    "daily": 10,
    "weekly": 10,
    "monthly": 10,
    "yearly": 10,
})


function deleteUntagged(revisionsData) {
    var promises = [];
    for (var i in revisionsData) {
        var e = revisionsData[i]
        if (e.tags.length == 0) {
            promises.push(fetch(`/api/revision/${e.number}`, {
                method: 'DELETE'
            }));
        }
    }
    Promise.all(promises).then(() => {
        renderRevisions();
    })
}

html`
    <form class="row g-3">
        ${() => ["latest", "hourly", "daily", "weekly", "monthly", "yearly"].map(x => {
            let assign = function (value) {
                config[x] = value;
                saveSettings(config);
            }
            return html`
                <div class="col-md-2">
                    <label for="input${x}" class="col-form-label">${x}</label>
                    <input type="number" id="input${x}" class="form-control" min="0" value="10"
                           @input="${e => assign(e.target.value)}"/>
                </div>`
        })}
    </form>           
`(document.getElementById('inputs'))

// html`${() => JSON.stringify(config)}`
// (document.getElementById('inputs'))

function renderRevisions() {

    fetch("/api/revisions")
        .then((response) => response.json())
        .then((data) => {
                var revisionsData = data;
                for (var i in revisionsData) {
                    var revision = revisionsData[i];
                    revision.delete_button = true
                    delete revision.date_time;
                }
                document.getElementById('buttons').innerHTML = '';
                html`
                    Deletions can be simulated first. The delete button in the table is only marking it as deleted.
                    Apply Deletions will then rewrite the backup (but not delete old volumes, TODO)
                    <br/>
                    <button @click="${() => deleteUntagged(revisionsData)}">Delete Revisions without Tags</button>
                    <button @click="${() => reset()}">Reset Deletions</button>
                    <button @click="${() => applyDeletions()}">Apply Deletions</button>
                    <button @click="${() => stopServer()}">Stop Server</button>
                    <br/><br/>
                `(document.getElementById('buttons'))

                var printDeleteButton = function (cell, formatterParams, onRendered) {
                    return "<button>Delete</button>";
                };
                const badgeColors = {
                    'latest': 'primary',
                    'daily': 'success',
                    'weekly': 'warning',
                };
                var printTags = function (cell, formatterParams, onRendered) {
                    var tags = cell.getValue();
                    var out = "";
                    for (var i in tags) {
                        var tag = tags[i];
                        var prefix = tag.split('-')[0];
                        var color = badgeColors[prefix] || 'secondary'; // Default to secondary if no mapping found
                        out += `<span class="badge rounded-pill text-bg-${color}">${tag}</span> `;
                    }
                    return out;
                };
                var numberParams = {
                    decimal: ".",
                    thousand: "'",
                    symbol: "",
                    negativeSign: true,
                    precision: false,
                };
                const table = new Tabulator("#tab-table", {
                    data: revisionsData,
                    autoColumns: true,
                    autoColumnsDefinitions: [
                        {
                            field: "delete_button",
                            title: "",
                            formatter: printDeleteButton,
                            hozAlign: "center",
                            cellClick: function (e, cell) {
                                deleteRevision(cell.getRow().getData().number);
                            }
                        },
                        {field: "tags", title: "Tags", formatter: printTags, headerSort: false},
                        {field: "total_size", title: "Total Size", formatter: byteFormatter, hozAlign: "right"},
                        {
                            field: "number_of_files",
                            title: "Number of Files",
                            hozAlign: "right",
                            formatter: "money",
                            formatterParams: numberParams
                        },
                        {field: "timestamp", title: "Date", formatter: timestampFormatter},
                        {field: "number", title: "Revision", hozAlign: "right"},
                    ]
                });
            }
        )
}

renderRevisions();