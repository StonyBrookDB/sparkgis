/**
 *  query : connect to tcga_segmentation collection and retrive data
 *          by 'analysis_execution_id' and 'image.caseid',
 *          then read the points data, and parser them into WKT format,
 *          add an index starting from 1 to mark the record number
 *  author: Yangyang Zhu (yangyang.zhu@stonybrook.edu)
 *  time  : 2/10/2015
 */

function query() {
  db = db.getSiblingDB('u24_seer');

  var results = db.objects.find(
    {
      'provenance.analysis_execution_id' : 'test_seer1',
      'provenance.image.case_id' : 'SEER-HI-0001-BC100_1_1',  
    }
  );

  var img_meta = db.images.findOne({'case_id' : 'SEER-HI-0001-BC100_1_1'});
  /*results.forEach( printjson );*/
  var j;
  var doc, width, heigth, points, output;
  var img_doc = img_meta.next();
  width  = img_doc.width;
  height = img_doc.height;
  while (results.hasNext()) {
    doc = results.next();
    points = doc.geometry.coordinates[0][0];
    
    points = points.trim().split(/[\s,]+/);

    output = doc._id.str + '\t' + 'POLYGON ((';
    if (points.length > 1)
      output = output + Number(points[0]) * width + ' ' + Number(points[1]) * heigth; 
    for (j = 2; j < points.length; j++) {
      output = output + ',' + Number(points[j]) * width + ' ';
      j = j + 1; 
      output = output + Number(points[j]) * heigth;
    }
    /*if (execution_id == '/liz:vanner:sbu_definiens:2-13-2105/vanner/sbu_definiens/2-13-2015/00:00:00')
      output = output + ',' + Number(points[0]) * width + ' ' + Number(points[1]) * heigth; */
    output = output + '))'
    
    print(output);
  }
}
query();

