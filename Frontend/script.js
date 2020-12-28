google.charts.load('current', {
    callback: drawTable,
    packages: ['table', 'controls']
});

function drawTable() {
    AWS.config.update({
        region: "us-east-1",
        accessKeyId: "AKIAWUOQNXJQAW7VH2WI",
        secretAccessKey: "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"
    });

    var docClient = new AWS.DynamoDB.DocumentClient();

    var params = {
        TableName: "task3-transactions",
        ProjectionExpression: "user_id, transaction_id, amount, card_number, description, transaction_type, vendor, approval_status",
    };

    function onScan(err, data) {
        if (err) {
            document.getElementById('error-msg').innerHTML += "Unable to scan the table: " + "\n" + JSON.stringify(err, undefined, 2);
        } else {
            document.getElementById("transactions").innerText = data.Count;
            var mydata = new google.visualization.DataTable();
            mydata.addColumn('string', 'user_id');
            mydata.addColumn('string', 'transaction_id');
            mydata.addColumn('string', 'card_number');
            mydata.addColumn('string', 'amount');
            mydata.addColumn('string', 'description');
            mydata.addColumn('string', 'transaction_type');
            mydata.addColumn('string', 'vendor');
            mydata.addColumn('string', 'approval_status');

            // Print all the transactions
            data.Items.forEach(function(transaction) {
                mydata.addRows([
                    [transaction.user_id, transaction.transaction_id, transaction.card_number, transaction.amount, transaction.description, transaction.transaction_type, transaction.vendor, transaction.approval_status]
                ]);
            });

            var dashboard = new google.visualization.Dashboard(
                document.getElementById('dashboard_div')
            );

            var filterUser = new google.visualization.ControlWrapper({
                controlType: 'CategoryFilter',
                containerId: 'filter_user_div',
                options: {
                    filterColumnLabel: 'user_id',
                    ui: {
                        allowTyping: false,
                        allowMultiple: true,
                        sortValues: true
                    }
                }
            });

            var filterStatus = new google.visualization.ControlWrapper({
                controlType: 'CategoryFilter',
                containerId: 'filter_status_div',
                options: {
                    filterColumnLabel: 'approval_status',
                    ui: {
                        allowTyping: false,
                        allowMultiple: true,
                        sortValues: true
                    }
                }
            });

            var chart = new google.visualization.ChartWrapper({
                chartType: 'Table',
                containerId: 'chart_div',
                options: {
                    'height': 300
                }
            });


            dashboard.bind(filterUser, filterStatus);
            dashboard.bind(filterStatus, chart);
            dashboard.draw(mydata)
            google.visualization.events.addListener(chart, 'ready', myReadyHandler);
        }
    }
    docClient.scan(params, onScan);
}

function myReadyHandler() {
    var t = document.querySelector("table");
    t.setAttribute("class", "table table-bordered table-hover table-striped");

    // var btn = document.querySelector(".goog-menu-button");
    // btn.setAttribute("class", "dropdown");
}