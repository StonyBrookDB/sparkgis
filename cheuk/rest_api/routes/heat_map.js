

var bodyParser = require('body-parser');
var multiparty = require('multiparty');
var fs = require("fs");
var uuid = require('node-uuid');
var exec = require('child_process').exec;
var kue = require('kue');


 var express = require('express');
   var router = express.Router();



var queue = kue.createQueue({
	disableSearch: false
});


 

var redis = require('redis');
var redis_client = redis.createClient(6379, '127.0.0.1', {no_ready_check: true});

var config = require('config');
var dbConfig = config.get('Setting.dbConfig');
var localConfig = config.get('Setting.local');
var sparkConfig = config.get('Setting.spark');

var HOST = dbConfig.get('host')
var PORT = dbConfig.get('port')
var DBNAME = dbConfig.get('dbname')
var COLLECTION1 = dbConfig.get('collection1');



var LOCAL_PORT = localConfig.get('port')

var SPARK_CMD = sparkConfig.get('cmd1')
var SPARK_BIN = sparkConfig.get('bin')
var SPARK_CLASS = sparkConfig.get('class')
var SPARK_JAR = sparkConfig.get('jar')

var SPARK_CMD_HEATMAP = SPARK_BIN+" --class "+SPARK_CLASS+" "+SPARK_JAR;
var url = "mongodb://" + HOST + ":" + PORT + "/" + DBNAME;
var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');


// curl -X GET http://localhost:8127/api/job_status/144a7580-4d08-11e6-aaf9-5b317fa79b4e
router.get('/job_status/:id', function(req, res) {
  fun_job_status(req, res);
}); 


// curl -X GET http://localhost:8127/api/heat_map_result/ae997af0-4d08-11e6-aaf9-5b317fa79b4e

    router.get('/heat_map_result/:id',function(req, res) {
  fun_heat_map_result(req, res);
}); 


// curl --form "image=@results.json" --form "algos=yi-algorithm-v1,yi-algorithm-v11" --form "caseids=TCGA-02-0001-01Z-00-DX1" --form "metric=jaccard" --form "result_exe_id=cheuk_testabc" http://localhost:8127/api/upload_and_generate_heatmap


    router.post('/upload_and_generate_heatmap',function(req, res) {
  fun_upload_and_generate_heatmap(req, res);
}); 



// curl -X POST --data "algos=yi-algorithm-v1,yi-algorithm-v11&caseids=TCGA-02-0001-01Z-00-DX1&metric=jaccard&input=mongodb&output=mongodb&inputdb=u24_segmentation&inputcollection=results&outputdb=u24_segmentation&outputcollection=results&result_exe_id=cheuk_testabc" http://localhost:8127/api/generate_heatmap

    router.post('/generate_heatmap', function(req, res) {
  fun_generate_heatmap(req, res);
}); 










 var fun_job_status = function(req, res) {


	var job_id2 = req.params.id;
	console.log("taskId123: "+job_id2);


	redis_client.get(job_id2, function (err, reply) {
		if (err) {throw err};

		console.log(reply.toString());

		res.send('results: ' + JSON.stringify(reply.toString()));
	});



};








var fun_heat_map_result =  function(req, res) {

	var job_id2 = req.params.id;
	console.log("taskId: "+job_id2);



	MongoClient.connect(url, function(err, db) {
		assert.equal(null, err);

		var cursor = db.collection(COLLECTION1).find({
			'jobId': job_id2
		});

		// var count = db.collection(COLLECTION1).find({
		// 	'jobId': job_id2
		// }).count(function(e, count) {
		// 	console.log(count);
                 
  //             });

		cursor.toArray(function(err, results) {
			db.close();


			if (!err) {

				console.log("retrieved");
                      // console.log(result_json)
                      res.send('results: ' + JSON.stringify(results));

                  }
              });

	});
      // });

  };



 var fun_upload_and_generate_heatmap =  function(req, res) {


  	var form = new multiparty.Form();

  	form.parse(req, function(err, fields, files) {





  	 //    var params_array = ['algos', 'caseids', 'metric', 'input', 'output',   'inputdb', 'inputcollection' , 'outputdb' ,'outputcollection' ,'result_exe_id']
  		// var cmd_params = '';
  		// var uni_job_id = uuid.v1();

  		// for (var key of params_array) {
  		// 	var value = fields[key];
  		// 	var tmp = ' --' + key + ' ' + value;
  		// 	cmd_params = cmd_params + tmp;
  		// }



        var params_array = ['algos','caseids','metric','result_exe_id']


  var cmd_params ='';

 var uni_job_id = uuid.v1();

  for(var key of params_array)
  {
 
      var  value = fields[key];
      var tmp = ' --'+key+' '+value;

      cmd_params = cmd_params+tmp;
    }

    cmd_params = cmd_params +" --upload yes"+" --uid "+uni_job_id
 
  		console.log("upload_and_get_heatmap_cmd/;   " + cmd_params);

  		var ele = files.image
  		console.log(files)
  		var tmp_path = ele[0].path

  		var job = queue.create('upload_and_get_heatmap', {
  			title: uni_job_id,
  			tmp_path: tmp_path,
  			cmd_params: cmd_params
  		}).save(function() {
  			console.log("unique_job_idaaa: " + uni_job_id)
  			update_job_status(job)
  			res.send('job_id: ' + uni_job_id);
  		});
  	});
  };



  var fun_generate_heatmap =  function(req, res) {


  	var data_body = req.body;

  	console.log(data_body);

            var params_array = ['algos', 'caseids', 'metric', 'input', 'output',   'inputdb', 'inputcollection' , 'outputdb' ,'outputcollection' ,'result_exe_id']


  	var cmd_params = '';

  	var uni_job_id = uuid.v1();

  	for (var key of params_array) {


  		var value = data_body[key];
  		var tmp = ' --' + key + ' ' + value;

  		cmd_params = cmd_params + tmp;
  	}


  	cmd_params = cmd_params + " --uid " + uni_job_id
  	console.log("get_heat_map: " + cmd_params);




  	var job = queue.create('heat_map', {
  		title: uni_job_id,
  		cmd_params: cmd_params

  	}).save(function() {
  		update_job_status(job)
  		res.send('job_id: ' + uni_job_id);
  	});


  }





  queue.process('upload_and_get_heatmap', 2, function(job, done) {
  	var tmp_path = job.data.tmp_path


  	var command1 = "mongoimport --host " + HOST + ":" + PORT + " --db " + DBNAME + " --collection " + COLLECTION1 + " --file " + tmp_path


      // var command1 ="mongoimport --host localhost:27017 --db u24_segmentation --collection "+COLLECTION1+" --file " + tmp_path
      exec(command1, function(error, stdout, stderr) {
      	fun_get_heat_map(job, done);

      });





  });



 
  queue.process('heat_map', 2, function(job, done) {

  	fun_get_heat_map(job, done);


  });



  var fun_get_heat_map = function(job, done) {




  	var job_title = job.data.title
  	var cmd_params = job.data.cmd_params

  	console.log("title: " + job_title)
  	console.log("cmd_params: " + cmd_params)

  	var job_file = "log/"+job_title + ".txt"
  	var log_cmd = " > " + job_file


  	exec(SPARK_CMD_HEATMAP + cmd_params + log_cmd, function(error, stdout, stderr) {

  		console.log('stdout: ' + stdout);
  		console.log('!!!!!!!!!!!!!!: ');
  		console.log('stderr: ' + stderr);


  		var output = fs.readFileSync(job_file).toString().split("\n");
  		var len = output.length
  		var status = output[len - 2];



  		if (status == 'completed') {
  			console.log(status);
  			done()
  		} else {
  			var err = new Error(stderr);
  			done(err);
  			console.log("done error");

  		}


  	});
  }





  var update_status_by_jobid = function(status, jobid) {
  	redis_client.set(jobid, status, redis.print);
  }




  var update_job_status = function(job) {

  	var uni_job_id = job.data.title;

  	console.log("uni_job_id:   " + uni_job_id)

  	job.on('complete', function(result) {
  		console.log('Job completed ');
  		update_status_by_jobid('complete', uni_job_id)

  	}).on('failed attempt', function(errorMessage, doneAttempts) {
  		console.log('Job failed');

  	}).on('start', function(errorMessage, doneAttempts) {
  		console.log('Job started');
  		update_status_by_jobid('start', uni_job_id)

  	}).on('enqueue', function(result) {

  		console.log('enqueue12');
  		update_status_by_jobid('enqueue', uni_job_id);


  	}).on('progress', function(progress, data) {
          // console.log('\r  job #' + job.id + ' ' + progress + '% complete with data ', data );
          console.log("ssssssssss")
      });
  }


module.exports = router;