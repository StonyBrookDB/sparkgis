#include <iostream>
#include <jni.h>
#include "jni_JNIWrapper.h"
#include "include/partitionMapperJoin.hpp"
#include "include/resque.hpp"

using namespace std;

//PartitionMapperJoin* pmj = NULL;

/*
 * MUST CALL buildIndex() prior to calling this function
 * Get tile id for passed geometry string
 */
JNIEXPORT jobjectArray JNICALL Java_jni_JNIWrapper_partitionMapperJoin
(JNIEnv *env, jclass c, jstring line, jint geom_id, jobjectArray partfile)
//(JNIEnv *env, jclass c, jstring line)
{
  PartitionMapperJoin pmj(geom_id);
  int psize = env->GetArrayLength(partfile);
  // generate tiles from partfile
  for (int i=0; i<psize; ++i){
    jstring j_str = (jstring) env->GetObjectArrayElement(partfile, i);
    string c_str = env->GetStringUTFChars(j_str, NULL);
    pmj.gen_tile(c_str);
    // free memory to assist garbage collection by jvm
    env->DeleteLocalRef(j_str);
  }
  pmj.build_index();
  
  string in_line = env->GetStringUTFChars(line, NULL);
  //cout << in_line << endl;
  vector<string> hits = pmj.map_line(in_line);
  int size = hits.size();

  jclass clazz = env->FindClass("java/lang/String");
  jobjectArray objarray = env->NewObjectArray(size ,clazz ,0);
  
  for(int i = 0; i < size; i++) {
    string s = hits[i]; 
    jstring js = (env)->NewStringUTF(s.c_str());
    (env)->SetObjectArrayElement(objarray , i , js);
  }
  return objarray;    
}

/* RESQUE JNI INTERFACE */
JNIEXPORT jobjectArray JNICALL Java_jni_JNIWrapper_resque
  (JNIEnv *env , jclass c, jobjectArray data, jstring predicate, jint geomid1, jint geomid2)
{
  int size = env->GetArrayLength(data);
  //initialize resque
  string p_str = env->GetStringUTFChars(predicate, NULL);
  
  Resque resq(p_str, geomid1, geomid2);
  // populate datasets for join
  for (int i=0; i<size; ++i){
    
    jstring j_str = (jstring) env->GetObjectArrayElement(data, i);
    string c_str = env->GetStringUTFChars(j_str, NULL);
    //cout << c_str << endl;
    resq.populate(c_str);
    // free memory to assist garbage collection by jvm
    env->DeleteLocalRef(j_str);
  }

  // data results for this tile
  vector<string> hits = resq.join_bucket();
  size = hits.size();
  //if (size > 0)
  //  cout << "Return size: " << size << endl;

  //return as String[] back to Java
  jclass clazz = env->FindClass("java/lang/String");
  jobjectArray objarray = env->NewObjectArray(size ,clazz ,0);
  
  for(int i = 0; i < size; i++) {
    string s = hits[i]; 
    jstring js = (env)->NewStringUTF(s.c_str());
    (env)->SetObjectArrayElement(objarray , i , js);
  }
  return objarray;
}

JNIEXPORT jdouble JNICALL Java_jni_JNIWrapper_resqueTileDice
(JNIEnv* env, jclass c, jobjectArray data, jstring predicate, jint geomid1, jint geomid2)
{
  int size = env->GetArrayLength(data);
  //initialize resque
  string p_str = env->GetStringUTFChars(predicate, NULL);
  
  Resque resq(p_str, geomid1, geomid2);
  // populate datasets for join
  for (int i=0; i<size; ++i){
    
    jstring j_str = (jstring) env->GetObjectArrayElement(data, i);
    string c_str = env->GetStringUTFChars(j_str, NULL);
    //cout << c_str << endl;
    resq.populate(c_str);
    // free memory to assist garbage collection by jvm
    env->DeleteLocalRef(j_str);
  }
  
  double tile_dice_result = resq.tile_dice();
  return tile_dice_result;

  //string tile_dice_result = resq.tile_dice();
  //jstring js = (env)->NewStringUTF(tile_dice_result.c_str());
  //return js;
}

/* 
 * Alternate approach 
 * Initialize Resque class object and return back its pointer. Use class pointer to populate data
 * For each string in java, call a jni function to populate the data in Resque class variables
 * when key changes call join_bucket() similar to original Hadoop-GIS
 * Disadvantage: Too many JNI calls
 */
// JNIEXPORT jlong JNICALL Java_jni_JNIWrapper_initResque
//   (JNIEnv *env, jclass c, jstring predicate, jint geomid1, jint geomid2)
// {
//   string p_str = env->GetStringUTFChars(predicate, NULL);
//   Resque *r = new Resque(p_str, geomid1, geomid2);
//   //Resque(p_str, geomid1, geomid2);
//   return (long) r;
// }

// JNIEXPORT jobjectArray JNICALL Java_jni_JNIWrapper_resque2
// (JNIEnv *env, jclass c, jlong objPtr, jstring line)
// {
 
//   Resque *resq = (Resque *)objPtr;
  
//   string c_str = env->GetStringUTFChars(line, NULL);
//   // if not same tileID, call join bucket
//   if (!resq->populate2(c_str)){
//     //vector<string> hits = resq->join_bucket();
//     //if (hits.size() > 0)
//     //  cout << "Got something ..." << hits.size() << endl;
//     // reset object
//     resq->reset();
//     // corner case: populate extra data
//     resq->populate_extra();
//   }
//   else
//     vector<string> hits = vector<string>();  
// }

/******************************** CODE BACKUP ***********************************/

// JNIEXPORT void JNICALL Java_jni_JNIWrapper_nativeFoo (JNIEnv *env, jclass c)
// {
//   cout << "C++ BAIG WAS HERE" << endl;
// }
// /*
//  * MUST BE CALLED PRIOR TO CALLING any other function
//  * Create index from partfile. 
//  * Return index pointer as long
//  */
// JNIEXPORT jlong JNICALL Java_jni_JNIWrapper_buildIndex
//   (JNIEnv *env, jclass c, jobjectArray partfile, jint geom_id)
// {
//   // // check if index created. If yes, no need to create again
//   // if (pmj == NULL){
//   //   cout << "Building index" << endl;
//   //   pmj = new PartitionMapperJoin(geom_id);
//   //   int size = env->GetArrayLength(partfile);
//   //   // generate tiles from partfile
//   //   for (int i=0; i<size; ++i){
//   //     jstring j_str = (jstring) env->GetObjectArrayElement(partfile, i);
//   //     string c_str = env->GetStringUTFChars(j_str, NULL);
//   //     pmj->gen_tile(c_str);
//   //     // free memory to assist garbage collection by jvm
//   //     env->DeleteLocalRef(j_str);
//   //   }
//   //   pmj->build_index();
//   //}
//   return -1;
// }

// JNIEXPORT void JNICALL Java_jni_JNIWrapper_pmjGarbageCollection
//   (JNIEnv *env , jclass c, jlong idx_ptr)
// {
//   cout << "Not Implemented Yet ..." << endl;
//   //partitionMapperJoin_free_mem(idx_ptr);
// }
