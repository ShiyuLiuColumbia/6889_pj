var data_points = [];
var score = 0;
$(function () {

    var average_points = [];
    var variance_points = [];
    var window_points = [];
    var wv_points = [];
    var trend_points = [];
    var price_points = [];
    var predictions = [];


    $("#chart").height($(window).height() - $("#header").height() * 2);
    // $("#chart2").height($(window).height() - $("#header").height() * 2);
    // $("#chart3").height($(window).height() - $("#header").height() * 2);
    $("#show_average").on('click', function() {
        // $("#chart").toggle();
        data_points = average_points;
        chart.yAxis
        .axisLabel('Average Price');

    });
    $("#show_variance").on('click', function() {
        data_points = variance_points;
        chart.yAxis
        .axisLabel('Variance');

    });
    $("#show_window").on('click', function() {
        data_points = window_points;
        chart.yAxis
        .axisLabel('Average of last 30 ticks');

    });
    $("#show_window_variance").on('click', function() {
        data_points = wv_points;
        chart.yAxis
        .axisLabel('Variance of last 30 ticks');

    });
    $("#show_trend").on('click', function() {
        data_points = trend_points;
        chart.yAxis
        .axisLabel('Trend of last 30 ticks');

    });
    $("#show_price").on('click', function() {
        data_points = price_points;
        chart.yAxis
        .axisLabel('Price');

    });
    $(document.body).on('click', '.stock-label', function () {
        "use strict";
        var symbol = $(this).text();
        $.ajax({
            url: 'http://localhost:5000/' + symbol,
            type: 'DELETE'
        });
        $(this).remove();
        var i = getSymbolIndex(symbol, average_points);
        average_points.splice(i, 1);
        var j = getSymbolIndex(symbol, variance_points);
        variance_points.splice(j, 1);
        var k = getSymbolIndex(symbol, window_points);
        window_points.splice(k, 1);
        var m = getSymbolIndex(symbol, wv_points);
        wv_points.splice(m, 1);
        var n = getSymbolIndex(symbol, trend_points);
        trend_points.splice(n, 1);
        var l = getSymbolIndex(symbol, price_points);
        trend_points.splice(l, 1);
        console.log(data_points);
    });

    $("#add-stock-button").click(function () {
        "use strict";
        var symbol = $("#stock-symbol").val();
        var l = getSymbolIndex(symbol, price_points);
        var right = 0;
        for (var a = 1; a < predictions.length - 10; a++) {
            if (price_points[l][i+10] > price_points[l][i] && predictions[i] == 1){
                right++;
            }
        }
        score = right/(predictions.length - 10);
        var demoP = document.getElementById("score");
        demoP.innerHTML = "Score is " + score + "%";

        $.ajax({
            url: 'http://localhost:5001/' + symbol,
            type: 'POST'
        });

        $("#stock-symbol").val("");
        average_points.push({
            values: [],
            key: symbol
        });
        variance_points.push({
            values: [],
            key: symbol
        });
        window_points.push({
            values: [],
            key: symbol
        });
        wv_points.push({
            values: [],
            key: symbol
        });
        trend_points.push({
            values: [],
            key: symbol
        });
        price_points.push({
            values: [],
            key: symbol
        });

        $("#stock-list").append(
            "<a class='stock-label list-group-item small'>" + symbol + "</a>"
        );

        console.log(data_points);
    });

    function getSymbolIndex(symbol, array) {
        "use strict";
        for (var i = 0; i < array.length; i++) {
            if (array[i].key == symbol) {
                return i;
            }
        }
        return -1;
    }

    var chart = nv.models.lineChart()
        .interpolate('monotone')
        .margin({
            bottom: 100
        })
        .useInteractiveGuideline(true)
        .showLegend(true)
        .color(d3.scale.category10().range());

    chart.xAxis
        .axisLabel('Time')
        .tickFormat(formatDateTick);

    chart.yAxis
        .axisLabel('Price');

    nv.addGraph(loadGraph);

    function loadGraph() {
        "use strict";
        d3.select('#chart svg')
            .datum(data_points)
            .transition()
            .duration(5)
            .call(chart);

        nv.utils.windowResize(chart.update);
        return chart;
    }

    function newDataCallback(message) {
        "use strict";
        var parsed = JSON.parse(message)['value'];
        console.log(JSON.parse(message));
        parsed = JSON.parse(parsed);
        var symbol = parsed['symbol'];
        if (JSON.parse(message)['topic'] == "average-stock-price"){

            var timestamp = parsed['timestamp'];
            var average = parsed['average'];
            var variance = parsed['variance'];


            var point = {};
            point.x = timestamp;
            point.y = average;

            console.log(point);

            var i = getSymbolIndex(symbol, average_points);

            average_points[i].values.push(point);
            if (average_points[i].values.length > 100) {
                average_points[i].values.shift();
            }
            var pointv = {};
            pointv.x = timestamp;
            pointv.y = variance;

            console.log(pointv);

            var j = getSymbolIndex(symbol, variance_points);

            variance_points[j].values.push(pointv);
            if (variance_points[j].values.length > 100) {
                variance_points[j].values.shift();
            }
        }


        if (JSON.parse(message)['topic'] == "window-stock-price") {

            var pointw = {};
            pointw.x = parsed["timestamp"];
            pointw.y = parsed["average"];



            var k = getSymbolIndex(symbol, window_points);
            window_points[k].values.push(pointw);
            if (window_points[k].values.length > 100) {
                window_points[k].values.shift();
            }
            var pointwv = {};
            pointwv.x = parsed["timestamp"];
            pointwv.y = parsed["variance"];



            var m = getSymbolIndex(symbol, wv_points);
            wv_points[m].values.push(pointwv);
            if (wv_points[m].values.length > 100) {
                wv_points[m].values.shift();
            }
            var pointwt = {};
            pointwt.x = parsed["timestamp"];
            pointwt.y = parsed["trend"];



            var n = getSymbolIndex(symbol, trend_points);
            trend_points[n].values.push(pointwt);
            if (trend_points[n].values.length > 100) {
                trend_points[n].values.shift();
            }
        }
        if (JSON.parse(message)['topic'] == "stock-analyzer") {
            var pointp = {};
            pointp.x = parsed[0]["LastTradeDateTime"];
            pointp.y = parsed[0]["LastTradePrice"];
            var l = getSymbolIndex(parsed[0]['StockSymbol'], price_points);
            price_points[l].values.push(pointp);
            if (price_points[l].values.length > 100) {
                price_points[l].values.shift();
            }
        }

        loadGraph();
    }

    function formatDateTick(time) {
        "use strict";
        var date = new Date(time * 1000);
        return d3.time.format('%H:%M:%S')(date);
    }

    var socket = io();

    // - Whenever the server emits 'data', update the flow graph
    socket.on('data', function (data) {
    	newDataCallback(data);
    });
});
