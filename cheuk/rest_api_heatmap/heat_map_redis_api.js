var express = require('express');

// var router = express.Router();
var app = express();
var bodyParser = require('body-parser');
var multiparty = require('multiparty');

var config = require('config');

var route_heat_map = require('./routes/heat_map');

var redis = require('redis');
var redis_client =
    redis.createClient(6379, '127.0.0.1', {no_ready_check : true});

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended : true}));

var dbConfig = config.get('Setting.dbConfig');
var localConfig = config.get('Setting.local');
var sparkConfig = config.get('Setting.spark');

var HOST = dbConfig.get('host') var PORT = dbConfig.get('port') var DBNAME =
    dbConfig.get('dbname')

        var LOCAL_PORT = localConfig.get('port')

// router.get('/job_status/:id', route_heat_map.job_status);
// router.get('/heat_map_result/:id',route_heat_map.heat_map_result);
// router.post('/upload_and_get_heatmap',route_heat_map.upload_and_get_heatmap
// );
// router.post('/generate_get_heatmap', route_heat_map.generate_get_heatmap);

app.use('/api', route_heat_map);

var server = app.listen(LOCAL_PORT, function() {

  var host = server.address().address
  var port = server.address().port

  console.log("Example app listening at http://%s:%s", host, port)

})
