import {reactive, html} from 'https://esm.sh/@arrow-js/core';
import {formatBytes, byteFormatter} from '/js/utils.js';

fetch("/api/volume_usage")
    .then((response) => response.json())
    .then((usageData) => {
        var totalBytes = 0;
        var totalUnusedBytes = 0;
        for (let i = 0; i < usageData.length; i++) {
            usageData[i]["percent"] = (usageData[i]["unusedBytes"] * 100.0) / usageData[i]["length"];
            totalBytes += usageData[i]["length"];
            totalUnusedBytes += usageData[i]["unusedBytes"];
            usageData[i]["keep"] = null
        }
        html`${formatBytes(totalBytes)} unused ${formatBytes(totalUnusedBytes)}`(document.getElementById('stats'))

        var table = new Tabulator("#usage", {
            data: usageData, //assign data to table
            autoColumns: true, //create columns from data field names
            autoColumnsDefinitions: [
                {title: "Length", field: "length", formatter: byteFormatter},
                {title: "Unused", field: "unusedBytes", formatter: byteFormatter},
                {
                    title: "Unused Percent", field: "percent", formatter: "money", formatterParams: {
                        decimal: ".",
                        thousand: "'",
                        symbol: " %",
                        symbolAfter: "p",
                        negativeSign: true,
                        precision: 0,
                    }
                },
                {
                    title: "Keep", field: "keep", editor: true, formatter: "tickCross", editorParams: {
                        tristate: false,
                    }
                }
            ],
            initialSort: ([
                {column: "percent", dir: "desc"},
            ]),
        });
    })