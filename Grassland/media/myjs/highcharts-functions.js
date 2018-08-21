function plot_doy_data(cont, data_file, var_name, stat, point){
	 $.getJSON( data_file, function( data ) {
		var ts_data = []
		for (var doy_idx=0; doy_idx < data[var_name].length; doy_idx++){
			ts_data.push([doy_idx + 1, data[var_name][doy_idx][stat]]);
		}
        Highcharts.chart(cont, {
            chart: {
                zoomType: 'x'
            },
			exporting: {
        		filename: point + '_' + var_name + '_' + stat
    		},
            title: {
                text: point
            },
            subtitle: {
                //text: 'Click and drag in the plot area to zoom in'
				text: ''
            },
            xAxis: {
                title: {
                    text: 'Day of Year'
                },
				tickInterval: 10
            }, 
            yAxis: {
                title: {
                    text: var_name + ' ' + stat
                }
            },
            legend: {
                layout: 'vertical',
                align: 'right',
                verticalAlign: 'middle'
            },
            series: [{
                name: var_name + ' ' + stat,
                data: ts_data
            }]
        });
    });
}

